const {Client} = require('@elastic/elasticsearch');
const {dedup, ensureThunkSync} = require('@raychee/utils');
const {get} = require('lodash');
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
        delete elasticSearch._bulkLoader.bulkByJobId[jobId];
        delete elasticSearch._bulkLoader.errorByJobId[jobId];
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
    return get(logger, ['config', '_id'], '');
}

class BulkLoader {
    constructor(elastic) {
        this.elastic = elastic;
        this.indices = {};
        this.updating = {};
        this.committing = undefined;
        this.errorByJobId = {};
        this.bulkByJobId = {};
        
        this._ensureIndex = dedup(BulkLoader.prototype._ensureIndex.bind(this), {key: (_, index) => index});
    }

    async index(logger, index, source, {debug, createIndex} = {}) {
        while (this.committing) {
            await this.committing;
        }
        const jobId = getJobId(logger);
        this.bulkByJobId[jobId] = this.bulkByJobId[jobId] || [];
        while (this.bulkByJobId[jobId].length >= this.elastic.options.bulk.batchSize) {
            if (!this.committing) {
                this.committing = this._commit(logger, {debug}).finally(() => this.committing = undefined);
            }
            await this.committing;
        }
        const error = this.errorByJobId[jobId];
        if (error) {
            throw error;
        }
        this.bulkByJobId[jobId].push({index, source, createIndex});
    }

    async flush(logger, {debug} = {}) {
        logger = logger || this.elastic.logger;
        const jobId = getJobId(logger);
        while ((this.bulkByJobId[jobId] || []).length > 0) {
            if (!this.committing) {
                this.committing = this._commit(logger, {debug}).finally(() => this.committing = undefined);
            }
            await this.committing;
        }
        const updating = Object.values(this.updating);
        if (updating.length > 0) {
            await Promise.all(updating);
        }
        const error = this.errorByJobId[jobId];
        if (error) {
            delete this.errorByJobId[jobId];
            throw error;
        }
    }

    async _commit(logger, {debug} = {}) {
        const opId = uuid4();
        const jobId = getJobId(logger);
        const bulk = this.bulkByJobId[jobId] || [];
        if (bulk.length <= 0) return;
        while (Object.keys(this.updating).length >= this.elastic.options.bulk.concurrency) {
            if (debug || this.elastic.options.otherOptions.debug) {
                logger.debug('Wait for concurrency before indexing a batch: id = ', opId, ', size = ', bulk.length, '.');
            }
            await Promise.race(Object.values(this.updating));
        }
        const error = this.errorByJobId[jobId];
        if (error) {
            throw error;
        }
        if (debug || this.elastic.options.otherOptions.debug) {
            logger.debug(
                'Index a batch: id = ', opId, ', size = ', bulk.length,
                ', concurrency = ', Object.keys(this.updating).length + 1, '/', this.elastic.options.bulk.concurrency, '.'
            );
        }
        this.updating[opId] = this._index(logger, bulk)
            .catch(e => this.errorByJobId[jobId] = e)
            .finally(() => {
                delete this.updating[opId];
                if (debug || this.elastic.options.otherOptions.debug) {
                    logger.debug(
                        'Complete indexing a batch: id = ', opId, ', size = ', bulk.length,
                        ', concurrency = ', Object.keys(this.updating).length, '/', this.elastic.options.bulk.concurrency, '.'
                    );
                }
            });
        this.bulkByJobId[jobId] = [];
    }
    
    async _index(logger, bulk) {
        const indicesToCreate = {};
        for (const {index: {index: {_index}}, createIndex} of bulk) {
            const createIndexRequest = createIndex ? ensureThunkSync(createIndex, _index) : {index: _index};
            const index = _index || createIndexRequest.index;
            indicesToCreate[index] = createIndexRequest;
        }
        for (const [index, createIndexRequest] of Object.entries(indicesToCreate)) {
            await this._ensureIndex(logger, index, createIndexRequest);
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
        let [params, options] = this._validateArgs(logger, args);
        for (let trial = 0; trial <= this.options.bulk.maxRetriesForTooManyRequests; trial++) {
            const resp = await this._operate(logger, (client) => client.bulk(params, options));
            const body = this.options.request.fullResponse ? resp.body : resp;
            if (body.errors) {
                let i = 0, hasErrorOtherThanTooManyRequests = false;
                const retry = [], errorItems = [];
                for (const item of body.items) {
                    const [[action, {status, error}]] = Object.entries(item);
                    if (error) {
                        errorItems.push(item);
                        if (status !== 429) {
                            hasErrorOtherThanTooManyRequests = true;
                        }
                        retry.push(params.body[i]);
                        if (['create', 'index', 'update'].includes(action)) {
                            retry.push(params.body[i + 1]);
                        }
                    }
                    i++;
                    if (['create', 'index', 'update'].includes(action)) i++;
                }
                if (hasErrorOtherThanTooManyRequests) {
                    logger.fail('plugin_elasticsearch_bulk_error', errorItems.filter(item => {
                        const [{status}] = Object.values(item);
                        return status !== 429;
                    }));
                }
                if (trial < this.options.bulk.maxRetriesForTooManyRequests) {
                    if (retry.length <= 0) {
                        logger.crash(
                            'plugin_elasticsearch_bulk_crash', 'Bulk operation has failed because of too many requests, ',
                            'but no retryable bulk operations can be found.'
                        );
                    }
                    logger.warn(
                        'Bulk operation has failed because of too many requests. Will retry (',
                        trial + 1, '/', this.options.bulk.maxRetriesForTooManyRequests,
                        ') in ', this.options.bulk.retryIntervalForTooManyRequests, ' seconds.',
                    );
                    await logger.sleep(this.options.bulk.retryIntervalForTooManyRequests);
                    params = {...params, body: retry};
                } else {
                    logger.fail('plugin_elasticsearch_bulk_error', errorItems);
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
            logger.crash('plugin_elasticsearch_invalid_args', 'No callback-style is allowed, please use promise-style instead.');
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
            logger.fail('plugin_elasticsearch_error', error);
        });
    }

}
