const {Client} = require('@elastic/elasticsearch');
const {dedup, BatchLoader} = require('@raychee/utils');


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
    constructor(logger, elastic, {createIndex} = {}) {
        this.logger = logger;
        this.elastic = elastic;
        this.createIndex = createIndex;
        this.indices = {};

        this.batchLoader = new Batchloader(async (indexAndDocs) =>{
            const indices = new Set(indexAndDocs.map(v => {
                let [{index:{_index}}] = v;
                return _index;
            }));
            let indexRequest;
            for (let index of indices) {
                if (this.createIndex) {
                    indexRequest = this.createIndex(index);
                    let {index} = indexRequest;
                } else {
                    indexRequest = {index: index}
                }
                if (index) await this.ensureIndex(this, index, indexRequest);
            }
            logger.info('The target index is all created and ready to start inserting the document');
            const bulk = indexAndDocs.flatMap(doc => doc);
            await this.elastic.bulk({bulk});
        }, {maxSize: this.elastic.options.batchSize, concurrency: this.elastic.options.concurrency, maxWait: 1000});

        this.ensureIndex = dedup(this._ensureIndex.bind(this, index, indexRequest), {key: index})
    }

    async load(logger, index, doc) {
        await this.batchLoader.load([index, doc]);
    }

    async _ensureIndex(logger, index, indexRequest) {
        let {exists, refreshInterval} = this.indices[index] || {};
        if (exists === undefined) {
            exists = await this.elastic.indices().exists({index: index});
        }
        if (!exists) {
            logger.info('Target index ', index, ' does not exist.');
            while (true) {
                try {
                    await this.elastic.index({
                        index: 'locks', id: `indices.create.${index}`, opType: 'create', body: {},
                    });
                    break;
                } catch (e) {
                    if (e.cause && e.cause.name === 'ResponseError' && e.cause.statusCode === 409) {
                        logger.info(
                            'There is a lock conflict for creating the target index ',
                            index, ', will re-check after 1 second.'
                        );
                        await this.logger.delay(1);
                    } else {
                        throw e;
                    }
                }
            }
            logger.info('Create index ', index, '.');
            if (createIndex){
                await this.elastic.indices().create(indexRequest);
            } else {
                await this.elastic.indices().create(indexRequest);
            }
            await this.elastic.delete({index: 'locks', id: `indices.create.${index}`});
            exists = true;
        }
        if (refreshInterval === undefined) {
            const resp = await this.elastic.indices().getSettings({
                index: index, name: 'index.refresh_interval'
            });
            const [{settings: {index: {refresh_interval} = {}} = {}} = {}] = Object.values(resp);
            if (refresh_interval) {
                refreshInterval = refresh_interval;
            }
        }
        if (refreshInterval !== '-1') {
            logger.info('Set target index ', index, '\'s refresh interval to -1.');
            await this.elastic.indices().putSettings({
                index: index,
                body: {index: {refresh_interval: '-1'}},
            });
            refreshInterval = '-1';
        }
        this.indices[index] = {...this.indices[index], exists, refreshInterval};
    }

}


class ElasticSearch {

    constructor(logger, options) {
        this.logger = logger;
        this.options = makeFullOptions(options);
        this.client = undefined;
        this.bulkLoaders = {};
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

    async bulkUpdate(logger, index, doc, {createIndex} = {}) {
        let bulkLoader;
        const key = createIndex.toString();
        if (!this.bulkLoaders || !this.bulkLoaders[key]) {
            bulkLoader = this.bulkLoaders[key]
        } else {
            bulkLoader = new BulkLoader(logger, this, createIndex);
            this.bulkLoaders[key] = bulkLoader;
        }
        await bulkLoader.load([index, doc])
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
