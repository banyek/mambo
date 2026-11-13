# How to build mambo

Build from the repository root with Go 1.25.5 or later and GNU Make.

### Build the mambo for the current architecture
If you want to build mambo for your current architecture, you can do that with a simple make command. 
This downloads any missing dependencies and builds the `mambo` binary.

    > make

### Download dependencies
If you want to download all the dependencies for development, but you don't want to build the app, just make deps

    > make deps

### Build for OSX (darwin)
You can explicit tell if the build target is OSX. 

    > make darwin

### Build for Linux
You can explicit tell if the build target is Linux. 

    > make linux
    
### Remove the built binary

Use `make clean` to remove the `mambo` binary.

    > make clean
