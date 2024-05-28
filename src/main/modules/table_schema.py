cm_event_state_updates={
    "type":"object",
    "properties":{
        "userId" :{"type":["string","null"]},
        "stateMachineId" :{"type":["string","null"]},
        "@version" :{"type":["string","null"]},
        "@timestamp" :{"type":["string","null"]},
        "updatedAt" :{"type":["integer","null"]},
        "username":{"type":["string","null"]},
        "notificationVersion":{"type":["string","null"]},
        "state": {
                  "type":"object",
                  "properties":{
                                "id":{"type":["string","null"]},
                   },
            "required":["id"],
         },
"reasons":{
                   "type":"array",
                   "items":{
                             "type":"object",
                             "properties":{
                                       "id":{"type":["string","null"]},
                                       "statusid":{"type":["string","null"]},
                                       "name":{"type":["string","null"]},
                                      },
                                },
         },
        "ids":{
                   "type":"array",
                   "items":{
                             "type":"object",
                             "properties":{
                                       "channelId":{"type":["string","null"]},
                                       "identifier":{"type":["string","null"]},
                                       "timestamp":{"type":["integer","null"]},
"payload": {
                                                    "type":"object",
                                                    "properties":{
                                                                    "schema":{  
                                                                            "type":"object",
                                                                            "properties":{
                                                                                "alert_id":{"type":["string","null"]},
                                                                                "event_type":{"type":["string","null"]},
										"customer_portfolio_country":{"type":["string","null"]},
                                                                            },
                                                                        "required":["alert_id","customer_id","customer_portfolio_region","customer_portfolio_product","customer_portfolio_channel","customer_portfolio_country","customer_portfolio_type","customer_portfolio_class"],
                                                                    },
                                                                },
                                                            "required":["schema"],
                                               },
                                      },
										"customer_portfolio_country":{"type":["string","null"]},
                                                                            },
                                                                        "required":["alert_id","customer_id","customer_portfolio_region","customer_portfolio_product","customer_portfolio_channel","customer_portfolio_country","customer_portfolio_type","customer_portfolio_class"],
                                                                    },
                                                                },
                                                            "required":["schema"],
                                               },
                                      },
"required":["identifier","channelId","payload"],
                                },
        },
        "note":{"type":["string","null"]},
    },
    "required":["@version","@timestamp","state","updatedAt","ids"],

}
