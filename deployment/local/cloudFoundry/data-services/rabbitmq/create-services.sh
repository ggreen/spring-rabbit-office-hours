cf create-service p.rabbitmq on-demand-plan rabbitmq -c '{ "plugins": { "rabbitmq_stream": true } }'
