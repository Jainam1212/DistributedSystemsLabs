LAB 1

## How to start (Mac/Ubuntu)
- ```brew services start mysql``` to start mysql server
- ```docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:4-management``` to connect docker ports to your local device ports - this would run rabbitmq queue
- ```ollama pull llama3.2:1b and ollama serve``` to start ai api, ollama serve might also give output port already in use which means ai api is open
- ```uvicorn producer:app --port 8080``` starts producer.py make sure to match terminal directory
- ```python consumer.py``` to start consumer.py else ```<python_venv_path> consumer.py```


## How to check if everything is working
- ```curl -X POST localhost:8080/process -H "Content-Type: application/json" \-d '{"text": "give your prompt here"}'```
- this return an id in format of ```{"id":"uuid"}```
- ```curl localhost:8080/result/uuid``` do this to get output
- you can also see logs in producer/consumer terminal or in rabbitmq ui at [localhost](http://localhost:15672/)



## How to start (Mac/Ubuntu)
- ```brew services stop mysql``` to stop mysql server
- ```docker ps``` then find rabbitmq:4-management id and ```docker stop <id>``` to stop docker rabbitmq queue
- ```sudo systemctl stop ollama``` to stop ai api
- rest python files can ce stopped by ctrl-c

