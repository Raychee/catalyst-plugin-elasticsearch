const {Client} = require('@elastic/elasticsearch');
const {dedup, limit, ensureThunkSync} = require('@raychee/utils');
const {isEmpty, get} = require('lodash');
const {v4: uuid4} = require('uuid');


module.exports = {
    type: 'elasticsearch',
    key(options) {
        return makeFullOptions(options);
    },
    async create(options) {
        return new ElasticSearch(this, options);
    },
    unload(elasticSearch, job) {
        const jobId = getJobId(job);
        delete elasticSearch.bulkLoaders[jobId];
    },
    async destroy(elasticSearch) {
        await elasticSearch._close();
    }
};


function makeFullOptions(
    {client = {}, bulk, request, searchAndScroll, otherOptions}
) {
    return {
        client,
        bulk: {
            retryIntervalForTooManyRequests: 1,
            maxRetriesForTooManyRequests: 0,
            batchSize: 200,
            concurrency: 1,
            ...bulk,
        },
        searchAndScroll: {
            scroll: '5m',
            ...searchAndScroll,
        },
        request: {
            fullResponse: false,
            ...request,
        },
        otherOptions: {
            showProgressEvery: undefined,
            ...otherOptions
        }
    }
}

function getJobId(logger) {
    return get(logger, ['config', 'id'], '');
}

class BulkLoader {
    constructor(elastic) {
        this.elastic = elastic;
        this.indices = {};
        this.updating = {};
        this.flushing = undefined;
        this.errors = [];
        this.bulk = [];
        
        this.index = limit(BulkLoader.prototype.index.bind(this), 1);
        this._ensureIndex = dedup(BulkLoader.prototype._ensureIndex.bind(this), {key: (_, index) => index});
    }

    async index(logger, index, source, {createIndex} = {}) {
        this.bulk.push({logger, index, source, createIndex});
        if (this.bulk.length >= this.elastic.options.bulk.batchSize) {
            if (this.flushing) {
                await this.flushing;
            }
            await this._commit(logger);
        }
    }

    async flush(logger) {
        if (!this.flushing) {
            this.flushing = this._flush(logger).finally(() => this.flushing = undefined);
        }
        await this.flushing;
    }

    async _commit(logger) {
        if (this.errors.length > 0) {
            throw this.errors[0];
        }
        const opId = uuid4();
        const bulk = this.bulk;
        if (bulk.length <= 0) return;
        while (Object.keys(this.updating).length >= this.elastic.options.bulk.concurrency) {
            logger.info('Wait for concurrency before indexing a batch: id = ', opId, ', size = ', bulk.length, '.');
            await Promise.race(Object.values(this.updating));
            if (this.errors.length > 0) {
                throw this.errors[0];
            }
        }
        logger.info(
            'Index a batch: id = ', opId, ', size = ', bulk.length,
            ', concurrency = ', Object.keys(this.updating).length + 1, '/', this.elastic.options.bulk.concurrency, '.'
        );
        this.updating[opId] = this._index(bulk)
            .catch(e => this.errors.push(e))
            .finally(() => {
                delete this.updating[opId];
                logger.info(
                    'Complete indexing a batch: id = ', opId, ', size = ', bulk.length,
                    ', concurrency = ', Object.keys(this.updating).length, '/', this.elastic.options.bulk.concurrency, '.'
                );
            });
        this.bulk = [];
    }
    
    async _flush(logger) {
        logger = logger || this.elastic.logger;
        if (this.bulk.length > 0) {
            await this._commit(logger);
        }
        if (!isEmpty(this.updating)) {
            await Promise.all(Object.values(this.updating));
            this.updating = {};
        }
        for (const [index, state] of Object.entries(this.indices)) {
            const {refreshInterval} = state;
            if (refreshInterval === '-1') {
                logger.info('Set target index ', index, '\'s refresh interval back to 1s.');
                await this.elastic.indices(logger).putSettings({
                    index, body: {index: {refresh_interval: '1s'}},
                });
                state.refreshInterval = '1s';
            }
        }
        if (this.errors.length > 0) {
            const [error] = this.errors;
            this.errors = [];
            throw error;
        }
    }

    async _index(bulk) {
        const bulkInfo = {};
        const [{logger}] = bulk;
        for (const {logger, index: {index: {_index}}, createIndex} of bulk) {
            const indexRequest = createIndex ? ensureThunkSync(createIndex, _index) : {index: _index};
            const index = _index || indexRequest.index;
            bulkInfo[index] = {logger, indexRequest};
        }
        for (let [index, {logger, indexRequest}] of Object.entries(bulkInfo)) {
            await this._ensureIndex(logger, index, indexRequest);
        }
        const body = bulk.flatMap(v => [v.index, v.source]);
        await this.elastic.bulk(logger, {body});
    }

    async _ensureIndex(logger, index, indexRequest) {
        logger = logger || this.elastic.logger;
        
        indexRequest = indexRequest || {index};
        const realIndex = indexRequest.index || index;
        let createdRealIndex = false;

        let {exists, refreshInterval} = this.indices[index] || {};
        if (exists === undefined) {
            exists = await this.elastic.indices(logger).exists({index});
        }
        if (!exists) {
            logger.info('Target index ', index, ' does not exist.');
            while (true) {
                try {
                    await this.elastic.index(logger, {
                        index: 'locks', id: `indices.create.${realIndex}`, opType: 'create', body: {},
                    });
                    break;
                } catch (e) {
                    if (e.cause && e.cause.name === 'ResponseError' && e.cause.statusCode === 409) {
                        logger.info(
                            'There is a lock conflict for creating the target index ',
                            index, ', will re-check after 1 second.'
                        );
                        await logger.delay(1);
                    } else {
                        throw e;
                    }
                }
            }
            logger.info('Create index ', index, '.');
            await this.elastic.indices(logger).create(indexRequest);
            await this.elastic.delete(logger, {index: 'locks', id: `indices.create.${realIndex}`});
            exists = true;
            createdRealIndex = true;
        }
        if (refreshInterval === undefined) {
            const resp = await this.elastic.indices(logger).getSettings({
                index: index, name: 'index.refresh_interval'
            });
            const [{settings: {index: {refresh_interval} = {}} = {}} = {}] = Object.values(resp);
            if (refresh_interval) {
                refreshInterval = refresh_interval;
            }
        }
        if (refreshInterval !== '-1') {
            logger.info('Set target index ', index, '\'s refresh interval to -1.');
            await this.elastic.indices(logger).putSettings({
                index: index,
                body: {index: {refresh_interval: '-1'}},
            });
            refreshInterval = '-1';
        }
        this.indices[index] = {...this.indices[index], exists, refreshInterval};
        if (createdRealIndex) {
            this.indices[realIndex] = this.indices[index];
        }
    }
}


class ElasticSearch {

    constructor(logger, options) {
        this.logger = logger;
        this.options = makeFullOptions(options);
        this.bulkLoaders = {};
        this.client = undefined;
    }

    async index(logger, ...args) {
        logger = logger || this.logger;
        args = this._validateArgs(logger, args);
        return await this._operate(logger, client => client.index(...args));
    }

    async delete(logger, ...args) {
        logger = logger || this.logger;
        args = this._validateArgs(logger, args);
        return await this._operate(logger, client => client.delete(...args));
    }

    async bulk(logger, ...args) {
        logger = logger || this.logger;
        args = this._validateArgs(logger, args);
        let resp;
        for (let trial = 0; trial <= this.options.bulk.maxRetriesForTooManyRequests; trial++) {
            resp = await this._operate(logger, (client) => client.bulk(...args));
            if (this.options.request.fullResponse) {
                resp = resp.body;
            }
            if (resp.errors) {
                const errorItems = resp.items
                    .map((item, pos) => ({pos, ...Object.values(item)[0]}))
                    .filter(({error}) => error);
                if (errorItems.some(item => item.status !== 429)) {
                    logger.fail('_es_bulk_error', errorItems);
                }
                if (trial < this.options.bulk.maxRetriesForTooManyRequests) {
                    logger.info(
                        'Bulk operation has failed because of too many requests. Will retry (',
                        trial + 1, '/', this.options.bulk.maxRetriesForTooManyRequests,
                        ') in ', this.options.bulk.retryIntervalForTooManyRequests, ' seconds.',
                    );
                } else {
                    logger.fail('_es_bulk_error', errorItems);
                }
            } else {
                return resp;
            }
        }
    }

    async bulkIndex(logger, ...args) {
        await this._getBulkLoader(logger).index(logger, ...args);
    }

    async bulkFlush(logger) {
        await this._getBulkLoader(logger).flush(logger);
    }
    
    async prepareIndexForBulk(logger, ...args) {
        await this._getBulkLoader(logger)._ensureIndex(logger, ...args);
    }

    async search(logger, ...args) {
        logger = logger || this.logger;
        args = this._validateArgs(logger, args);
        return await this._operate(logger, client => client.search(...args));
    }

    async scroll(logger, ...args) {
        logger = logger || this.logger;
        args = this._validateArgs(logger, args);
        return await this._operate(logger, client => client.scroll(...args));
    }

    async* searchAndScroll(logger, ...args) {
        logger = logger || this.logger;
        const [params, options] = this._validateArgs(logger, args);
        const scroll = params.scroll || this.options.searchAndScroll.scroll;
        params.scroll = scroll;
        let body = await this._operate(logger, client => client.search(params, options));
        let count = 0;
        while (true) {
            if (this.options.request.fullResponse) {
                body = body.body;
            }
            for (const {_source} of body.hits.hits) {
                yield _source;
                count++;
                if (count % this.options.otherOptions.showProgressEvery === 0) {
                    process.stdout.write('.');
                }
            }
            if (body._scroll_id && body.hits.hits.length > 0) {
                body = await this._operate(
                    logger, client => client.scroll({scroll_id: body._scroll_id, scroll}, options)
                );
            } else {
                break;
            }
        }
    }
    
    async deleteByQuery(logger, ...args) {
        logger = logger || this.logger;
        args = this._validateArgs(logger, args);
        return await this._operate(logger, client => client.deleteByQuery(...args));
    }

    indices(logger) {
        logger = logger || this.logger;
        return new Proxy({}, {
            get: (target, p) => {
                return async (...args) => {
                    args = this._validateArgs(logger, args);
                    return await this._operate(logger, client => client.indices[p](...args));
                }
            }
        });
    }

    tasks(logger) {
        logger = logger || this.logger;
        return new Proxy({}, {
            get: (target, p) => {
                return async (...args) => {
                    args = this._validateArgs(logger, args);
                    return await this._operate(logger, client => client.tasks[p](...args));
                }
            }
        });
    }

    async reindex(logger, ...args) {
        logger = logger || this.logger;
        args = this._validateArgs(logger, args);
        return await this._operate(logger, client => client.reindex(...args));
    }

    async _connect() {
        if (!this.client) {
            this.client = new Client(this.options.client);
        }
        return this.client;
    }

    async _close() {
        if (this.client) {
            const client = this.client;
            this.client = undefined;
            await client.close();
        }
    }
    
    _getBulkLoader(logger) {
        logger = logger || this.logger;
        const jobId = getJobId(logger);
        const bulkLoader = this.bulkLoaders[jobId] || new BulkLoader(this);
        this.bulkLoaders[jobId] = bulkLoader;
        return bulkLoader;
    }

    _validateArgs(logger, args) {
        let [params, options, callback] = args;
        logger = logger || this.logger;
        options = options || {};
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }
        if (callback != null) {
            logger.crash('_es_plugin_invalid_args', 'No callback-style is allowed, please use promise-style instead.');
        }
        return [params, options];
    }

    _operate(logger, fn) {
        logger = logger || this.logger;
        let promise = this._connect().then(fn);
        if (!this.options.request.fullResponse) {
            promise = promise.then(resp => resp.body);
        }
        return promise.catch(error => {
            logger.fail('_es_error', error);
        });
    }

}
