.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Run 'make init' to initialize the local environment"
	@echo "Run 'make run' to enter the docker environment"
	@echo "Run 'make deploy-env' to directly deploy the environment"
	@echo "Run 'make deploy-src' to directly deploy the source"


.PHONY: ensure-keytab
ensure-keytab:
ifeq ($(KEYTAB),)
	@echo "the variable KEYTAB is not set"
	@exit 1
endif
ifeq (,$(wildcard $(KEYTAB)))
	@echo "keytab file does not exists, looked in location: $$KEYTAB"
	@exit 1
endif


.PHONY: init
init: set-precommit-hook

.PHONY: set-precommit-hook
set-precommit-hook:
	echo "#\!/bin/sh\ndocker-compose run cmd black --check /app/src" > .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit


.PHONY: run
run: ensure-keytab
	docker-compose run -v $(KEYTAB):/etc/krb5.keytab cmd bash


.PHONY: deploy-env
deploy-env: ensure-keytab
	docker-compose run -v $(KEYTAB):/etc/krb5.keytab cmd deploy-env

.PHONY: deploy-src
deploy-src: ensure-keytab
	docker-compose run -v $(KEYTAB):/etc/krb5.keytab cmd deploy-src


.PHONY: db-init
db-init: ensure-keytab
	docker-compose run -v $(KEYTAB):/etc/krb5.keytab cmd db-init

