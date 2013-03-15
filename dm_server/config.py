#!/usr/bin/env python

config = {
    'storage_server': 
                    [
                     'server_1_ip',
                     'server_2_ip',
                     'server_3_ip',
                    ],
    'zruntime': 
              {'username' : 'membase', 
               'password' : 'm3mb@s3@p1t00l',
               'gameid' : 'membase',
               'env' : 'auto',
               'mcs_key_name' : 'ACTIVE_MCS',
               'retries' : 60,
              },
    'params':
            {'poll_interval' : 5,
             'log_level' : 'info',
            },
}

