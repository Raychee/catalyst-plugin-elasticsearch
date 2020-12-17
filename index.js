const {Client} = require('@elastic/elasticsearch');
const {dedup, ensureThunkSync} = require('@raychee/utils');
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


class BulkLoader {
    constructor(elastic) {
        this.elastic = elastic;
        this.indices = {};
        this.updating = {};
        this.committing = undefined;
        this.errors = [];
        this.bulk = [];
        
        this._ensureIndex = dedup(BulkLoader.prototype._ensureIndex.bind(this), {key: (_, index) => index});
    }

    async index(logger, index, source, {debug, createIndex} = {}) {
        while (this.committing) {
            await this.committing;
        }
        while (this.bulk.length >= this.elastic.options.bulk.batchSize) {
            if (!this.committing) {
                this.committing = this._commit(logger, {debug}).finally(() => this.committing = undefined);
            }
            await this.committing;
        }
        this.bulk.push({logger, index, source, createIndex});
    }

    async flush(logger, {debug} = {}) {
        logger = logger || this.elastic.logger;
        if (this.bulk.length > 0) {
            if (!this.committing) {
                this.committing = this._commit(logger, {debug}).finally(() => this.committing = undefined);
            }
            await this.committing;
        }
        if (!isEmpty(this.updating)) {
            await Promise.all(Object.values(this.updating));
        }
        if (this.errors.length > 0) {
            const [error] = this.errors;
            this.errors = [];
            throw error;
        }
    }

    async _commit(logger, {debug} = {}) {
        const opId = uuid4();
        const bulk = this.bulk;
        if (bulk.length <= 0) return;
        while (Object.keys(this.updating).length >= this.elastic.options.bulk.concurrency) {
            if (debug || this.elastic.options.otherOptions.debug) {
                logger.debug('Wait for concurrency before indexing a batch: id = ', opId, ', size = ', bulk.length, '.');
            }
            await Promise.race(Object.values(this.updating));
        }
        if (this.errors.length > 0) {
            const [error] = this.errors;
            this.errors = [];
            throw error;
        }
        if (debug || this.elastic.options.otherOptions.debug) {
            logger.debug(
                'Index a batch: id = ', opId, ', size = ', bulk.length,
                ', concurrency = ', Object.keys(this.updating).length + 1, '/', this.elastic.options.bulk.concurrency, '.'
            );
        }
        this.updating[opId] = this._index(bulk)
            .catch(e => this.errors.push(e))
            .finally(() => {
                delete this.updating[opId];
                if (debug || this.elastic.options.otherOptions.debug) {
                    logger.debug(
                        'Complete indexing a batch: id = ', opId, ', size = ', bulk.length,
                        ', concurrency = ', Object.keys(this.updating).length, '/', this.elastic.options.bulk.concurrency, '.'
                    );
                }
            });
        this.bulk = [];
    }
    
    async _index(bulk) {
        const bulkIndices = {};
        const [{logger}] = bulk;
        for (const {logger, index: {index: {_index}}, createIndex} of bulk) {
            const indexRequest = createIndex ? ensureThunkSync(createIndex, _index) : {index: _index};
            const index = _index || indexRequest.index;
            bulkIndices[index] = {logger, indexRequest};
        }
        for (const [index, {logger, indexRequest}] of Object.entries(bulkIndices)) {
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

        let {exists} = this.indices[index] || {};
        if (exists === undefined) {
            exists = await this.elastic.indices(logger).exists({index});
        }
        if (!exists) {
            logger.info('Target index ', index, ' does not exist.');
            const lockCreateIndex = `indices.create.${realIndex}`;
            await this._lock(logger, lockCreateIndex);
            logger.info('Create index ', index, '.');
            await this.elastic.indices(logger).create(indexRequest);
            await this._unlock(logger, lockCreateIndex)
            exists = true;
            createdRealIndex = true;
        }
        this.indices[index] = {...this.indices[index], exists};
        if (createdRealIndex) {
            this.indices[realIndex] = this.indices[index];
        }
    }
    
    async _lock(logger, lockId) {
        logger = logger || this.elastic.logger;
        while (true) {
            try {
                await this.elastic.index(logger, {
                    index: 'locks', id: lockId, opType: 'create', body: {},
                });
                break;
            } catch (e) {
                if (e.cause && e.cause.name === 'ResponseError' && e.cause.statusCode === 409) {
                    logger.info('There is a lock conflict for ', lockId, ', will re-check after 1 second.');
                    await logger.delay(1);
                } else {
                    throw e;
                }
            }
        }
    }
    
    async _unlock(logger, lockId) {
        await this.elastic.delete(logger, {index: 'locks', id: lockId});
    }
}


class ElasticSearch {

    constructor(logger, options) {
        this.logger = logger;
        this.options = makeFullOptions(options);
        this._bulkLoader = new BulkLoader(this);
        this._client = undefined;
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
        await this._bulkLoader.index(logger, ...args);
    }

    async bulkFlush(logger) {
        await this._bulkLoader.flush(logger);
    }
    
    async prepareIndexForBulk(logger, ...args) {
        await this._bulkLoader._ensureIndex(logger, ...args);
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
        if (!this._client) {
            this._client = new Client(this.options.client);
        }
        return this._client;
    }

    async _close() {
        if (this._client) {
            const client = this._client;
            this._client = undefined;
            await client.close();
        }
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
