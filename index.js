'use strict'

var Promise = require('bluebird'),
    mysql   = require('mysql2'),
    Cache = require('nscp'),
    cache = new Cache({ stdTTL: 3600000, ttlPercent: 70 }),
    hit = require('debug')('hq:cache:hit'),
    miss = require('debug')('hq:cache:miss'),
    debug = require('debug')('hq:mysql'),
    log,
    mysqlCluster

Promise.promisifyAll(mysql)
Promise.promisifyAll(require('mysql2/lib/pool_cluster').prototype)

module.exports = function(params) {
    let groups = params.conf.mysqlGroups,
        verify = []

    mysqlCluster = mysql.createPoolCluster()
    mysqlCluster.clusterExecute = execute
    mysqlCluster.cachedClusterExecute = cachedExecute
    mysqlCluster.makeKey = makeKey
    mysqlCluster.firstResult = firstResult
    Promise.promisifyAll(mysqlCluster)

    for (let i = 0; i < groups.length; i++) {
        mysqlCluster.add(groups[i].groupName, groups[i])
        verify.push(mysqlCluster.clusterExecute('SELECT 1 + ? AS ANSWER',[i],groups[i].groupName)
            .then(function(result){
                debug('connection to',groups[i].host,'established',result)
                return result
            }))
    }

    function makeKey(parts) {
        let out

        if ( Array.isArray(parts) ) {
            out = parts
        } else {
            out = Array.prototype.slice.call(arguments)
        }
        return out.join(':')
    }

    function firstResult(value) {
        return value[0] || value[0] === 0 ? value[0] : value
    }

    function cachedExecute(key, query, params, group) {
        let toReturn = cache.get(key)

        if (toReturn) {
            return Promise.resolve(toReturn)
        } else {
            return execute(query, params, group)
                .bind({key: key})
                .then(updateCache)
        }
    }

    function updateCache(result) {
        cache.set(this.key,result)
        return result
    }

    function execute(query, params, group) {
        return Promise.using(getConnection(group),onConnection)

        function onConnection(connection) {
             connection.config.namedPlaceholders = !Array.isArray(params)

            return connection.executeAsync(query,params)
                .spread(function(results,_) {
                    if ( Array.isArray(results) && results.length ) {
                        return results[0][0] && results[0][0].length < 1 ? results[0][0] : results[0]
                    } else {
                        return results
                    }
                })
        }
    }

    function getConnection(group) {
        group = group || '*'
        return mysqlCluster.getConnectionAsync(group)
            .disposer(function(connection) {
                connection.release()
            })
    }

    //verify atleast one connection
    return Promise.all(verify)
        .then(function(answers){
            debug('complete round robin: ',answers)
            return mysqlCluster
        })
        .catch(function(err) {
            console.log(err)
            process.exit(1)
        })

}
