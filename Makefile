SHELL := /bin/bash

.PHONY: help tools-restore format format-check

help:
	@printf '%s\n' \
		'make tools-restore  Restore pinned local .NET tools' \
		'make format         Format C# files with CSharpier' \
		'make format-check   Check C# formatting without changing files'

tools-restore:
	dotnet tool restore

format:
	@set -o pipefail; \
	git ls-files --cached --others --exclude-standard -z -- '*.cs' '*.csx' \
		| xargs -0 dotnet csharpier format

format-check:
	@set -o pipefail; \
	git ls-files --cached --others --exclude-standard -z -- '*.cs' '*.csx' \
		| xargs -0 dotnet csharpier check
