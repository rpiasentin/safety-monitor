SHELL := /bin/bash

.PHONY: help preflight preflight-fix controlled-pass post-deploy-guard

help:
	@echo "Safety Monitor utility targets:"
	@echo "  make preflight      Run zero-friction access preflight checks"
	@echo "  make preflight-fix  Show quick unblock commands"

preflight:
	@./tools/preflight_access.sh

preflight-fix:
	@echo "Quick unblock commands:"
	@echo "  gh auth status -h github.com || gh auth login -h github.com -p https --web"
	@echo "  git ls-remote --heads origin main"
	@echo "  ssh -i /Users/rpias/dev/vscode-dev-env/.notes_access/ssh/ct104_root_ed25519 -o IdentitiesOnly=yes root@192.168.2.105 'echo ok'"

controlled-pass:
	@SM_CONTROLLED_PASS=1 SM_CT104_FIX_OWNERSHIP=1 ./tools/preflight_access.sh

post-deploy-guard:
	@./tools/ct104_post_deploy_guard.sh
