const express = require('express');
const hdb = require('hdb');
const async = require('async');

const app = express();
const port = process.env.PORT || 3000;

var client = hdb.createClient({
  host: '10.146.34.145',
  port: 30041,
  user: 'guptar29',
  databaseName: 'DD2HUB',
  password: 'Welcome7'
});

client.on('error', function (err) {
  console.error('Network connection error', err);
});

app.get('/:dur_in_secs?', (req, res) => {
  if (req.method == 'GET'){
    if(req.params.dur_in_secs == undefined) {
      var duration_in_seconds = 1800
    } else {
      duration_in_seconds = Number(req.params.dur_in_secs);    
    }
    
    client.connect(function (err) {
      if (err) {
        return res.type("text/plain").status(500).send("ERROR: " + err.toString());
      }
      async.parallel({
        QUOTES_CREATED: function (cb) {
          client.prepare(`SELECT COUNT ( * ) as QUOTES_CREATED from crm_dih.crmd_orderadm_h
                        where client = 300
                        AND CREATED_AT >= TO_VARCHAR((ADD_SECONDS (current_utctimestamp, ?)),'YYYYMMDDHH24MISS')
                        AND object_type =  'BUS2000116'`, (err, statement) => {
              if (err) {
                return cb(err);
              }
              statement.exec([-duration_in_seconds], function (err, rows) {
                if (err) {
                  return cb(err);
                } else {
                  if (rows.length > 0) {
                    let data = rows[0];
                    for (let d in data) {
                      return cb(null, data[d]);
                    }
                  } else {
                    return cb();
                  }
                }
              });
            });
        },
        QUOTES_CHANGED: function (cb) {
          client.prepare(`SELECT COUNT ( * ) as QUOTES_CHANGED from crm_dih.crmd_orderadm_h
                        where client = 300
                        AND CHANGED_AT >= TO_VARCHAR((ADD_SECONDS (current_utctimestamp, ?)),'YYYYMMDDHH24MISS')
                        AND object_type =  'BUS2000116'`, (err, statement) => {
              if (err) {
                return cb(err);
              }
              statement.exec([-duration_in_seconds], function (err, rows) {
                if (err) {
                  return cb(err);
                } else {
                  if (rows.length > 0) {
                    let data = rows[0];
                    for (let d in data) {
                      return cb(null, data[d]);
                    }
                  } else {
                    return cb();
                  }
                }
              });
            });
        },
        ORDERS_CREATED: function (cb) {
          client.prepare(`SELECT COUNT ( * ) as ORDERS_CREATED from crm_dih.crmd_orderadm_h
                        where client = 300
                        AND CREATED_AT >= TO_VARCHAR((ADD_SECONDS (current_utctimestamp, ?)),'YYYYMMDDHH24MISS')
                        AND object_type =  'BUS2000115'`, (err, statement) => {
              if (err) {
                return cb(err);
              }
              statement.exec([-duration_in_seconds], function (err, rows) {
                if (err) {
                  return cb(err);
                } else {
                  if (rows.length > 0) {
                    let data = rows[0];
                    for (let d in data) {
                      return cb(null, data[d]);
                    }
                  } else {
                    return cb();
                  }
                }
              });
            });
        },
        SOLUTIONS: function (cb) {
          client.prepare(`select top 1 description as TOP_PRODUCTS, sum(prod_count) as TOTAL_SOLUTION_CREATED from
                        (
                        SELECT description, count(ordered_prod) as prod_count FROM crm_dih.crmd_orderadm_i
                                              where  client = 300
                                              AND CHANGED_AT >= TO_VARCHAR((ADD_SECONDS (current_utctimestamp, ?)),'YYYYMMDDHH24MISS')
                                              AND parent = '00000000000000000000000000000000'
                                              group by description order by prod_count desc
                        ) group by description`, (err, statement) => {
              if (err) {
                return cb(err);
              }
              statement.exec([-duration_in_seconds], function (err, rows) {
                if (err) {
                  return cb(err);
                } else {
                  if (rows.length > 0) {
                    let data = rows[0];
                    return cb(null, data);
                  } else {
                    return cb();
                  }
                }
              });
            });
        }
      }, (err, results) => {
        if (err) {
          res.type("text/plain").status(500).send(`ERROR: ${err.toString()}`);
        } else {
          client.end();
          res.type("application/json").status(200).send(JSON.stringify(results));
        }
      });
    });
  } else {    
    return res.type("text/plain").status(500).send("Only GET method allowed");
  }
});

app.listen(port, () => console.log(`Node REST service using express listening on port ${port}!`));