# Contributing to PocketBase

If you stumble on a problem that you want to fix, please consider opening a discussion with your suggested fix as inline code, git diff attachment (`git diff > mychange.diff`) or link to your fork **(if not obvious - LLM contributions are not welcome no matter of their validity)**.

This document describes how to modify and work with the codebase.

- [Prerequisites](#prerequisites)
- [Making changes in the Go code](#making-changes-in-the-go-code)
- [Making changes in the Superuser UI](#making-changes-in-the-superuser-ui)

## Prerequisites

- Go 1.27+ (for making changes in the Go code)
- Node 24+ (for making changes in the Superuser UI)

## Making changes in the Go code

PocketBase is distributed as a Go package, which means that in order to run the project you'll have to create a Go `main` program that imports the package.

The repository already includes such program, located in `examples/base/main.go`, that is also used for the prebuilt executables.

So, let's assume that you already done some changes in the PocketBase Go code and you want now to run them:

1. Navigate to `examples/base`
2. Run `go run main.go serve`

This will start a web server on `http://127.0.0.1:8090` with the embedded prebuilt Superuser UI from `ui/dist`. And that's it!

To run the tests, you can execute (while in the root project directory):

  ```sh
  go test ./...

  # or using the Makefile
  make test
  ```

## Making changes in the Superuser UI

PocketBase Superuser UI is a single-page application (SPA) built with Svelte and Vite.

To start the Superuser UI:

1. Navigate to the `ui` project directory
2. Run `npm install` to install the node dependencies
3. Start vite's dev server
   ```sh
   npm run dev
   ```

You could open the browser and access the running Superuser UI at `http://127.0.0.1:5173`.

Since the Superuser UI is just a client-side application, you need to have the PocketBase backend server also running in the background (either manually running the `examples/base/main.go` or download a prebuilt executable).

> By default, the Superuser UI is expecting the backend server to be started at `http://127.0.0.1:8090`, but you could change that by creating a new `ui/.env.development.local` file with `PB_BACKEND_URL = YOUR_ADDRESS` variable inside it.

Every change you make in the Superuser UI should be automatically reflected in the browser at `http://127.0.0.1:5173` without reloading the page.

When you are done with your changes, you have to build the Superuser UI with `npm run build` so that it can be embedded in the Go module.

And that's it - you can continue maintaining your own indipendent fork and/or suggest your changes to the upstream _(once again LLM contributions are not welcome)_.
