#!/usr/bin/env make

GOPATH=$(abspath .)/core/deps:$(abspath .)/core
BINPATH=$(abspath .)/core/deps/bin

default: ios android

init:
	go get -v -u \
		golang.org/x/mobile/cmd/gomobile \
		golang.org/x/mobile/cmd/gobind
	$(BINPATH)/gomobile init -ndk $(ANDROID_HOME)/ndk-bundle
	go get -v `go list -f '{{range .Imports}}{{.}}{{" "}}{{end}}' weasels`
ios:
	$(BINPATH)/gomobile bind -target ios \
		-o ./output/Weasels.framework \
		weasels
android:
	$(BINPATH)/gomobile bind -target android \
		-o ./output/weasels.aar \
		weasels
