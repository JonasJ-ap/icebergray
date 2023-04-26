install:
	rm -r venv
	python -m venv venv
	venv/bin/pip install -r requirements.txt

install-apple-silicon:
	rm -r venv
	/usr/bin/python3 -m venv venv
	venv/bin/pip install -r requirements.txt


test-integration:
	docker-compose -f test-docker/docker-compose-integration.yml kill
	docker-compose -f test-docker/docker-compose-integration.yml build
	docker-compose -f test-docker/docker-compose-integration.yml up -d
	sleep 30
	venv/bin/python -m pytest tests/