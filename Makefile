
.PHONY:
help:
	@echo "Run 'make init' to initialize the local environment"

.PHONY:
init: set-precommit-hook

.PHONY:
set-precommit-hook:
	echo "#\!/bin/sh\ndocker-compose run cmd black --check /app/src" > .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit

