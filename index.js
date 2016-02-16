'use strict'

const mysql = require('mysql2')
const Promise = require('bluebird')
const debug = require('debug')('hq:mysql')
const error = require('error')('hq:mysql:error')

module.exports = function (params) {
  const verify = []
  const groups = params.conf.mysqlGroups

  const mysqlCluster = mysql.createPoolCluster()
  mysqlCluster.clusterExecute = execute

  function execute(query, params, group) {
    group = group || '*'
    return new Promise((resolve, reject) => {
      mysqlCluster.getConnection(group, (err, connection) => {
        if (err) {
          return reject(err)
        }

        connection.config.namedPlaceholders = !Array.isArray(params)
        connection.query(query, params, (err, rows) => {
          connection.release()

          if (err) {
            return reject(err)
          }

          resolve(rows)
        })
      })
    })
  }

  for (let i = 0; i < groups.length; i++) {
    mysqlCluster.add(groups[i].groupName, groups[i])

    verify.push(mysqlCluster.clusterExecute('SELECT 1 + ? AS ANSWER', [i], groups[i].groupName)
      .then(result => debug('connection to', groups[i].host, 'established', result)))
  }

  return Promise.all(verify)
    .then(function (answers) {
      debug('complete round robin: ', answers)
      return mysqlCluster
    })
    .catch(function (err) {
      console.log(err)
      process.exit(1)
    })
}