<a  name="readme-top"></a>

<h3  align="center">Blockheaders/Transactions Tool</h3>
<p  align="center">

Tool to maintain Blockheaders/Transactions DB. Data is queried via RPC calls.

<!-- TABLE OF CONTENTS -->
<details>
<summary>Table of Contents</summary>
<ol>
<li><a  href="#getting-started">Getting Started</a>
<li><a  href="#prerequisites">Prerequisites</a></li>
<li><a  href="#installation">Installation</a></li>
<li><a  href="#usage">Usage</a></li>
<li><a  href="#debugging">Debugging</a></li>
</ol>
</details>
<!-- ABOUT THE PROJECT -->

# Getting Started

To get this application runninng locally, follow these steps.

## Prerequisites

What you would need:

- Rust

```
https://www.rust-lang.org/tools/install
```

<!-- USAGE EXAMPLES -->
## Usage

### Clone the repo

```sh
git clone https://github.com/OilerNetwork/fossil-headers-db.git
```

### Create a .env file in the project's root folder

_fossil-headers-db/.env_

```
DB_CONNECTION_STRING=<db_connection_string>
NODE_CONNECTION_STRING=<node_connection_string>
ROUTER_ENDPOINT=<router_endpoint_string>
RUST_LOG=<log_level> [optional]
```

### Running the indexer

```sh
cargo run --bin fossil_indexer
```

You can also build and run a release version:


```sh
cargo build --release
 ```

```sh
target/release/fossil_indexer
```


### Using Docker

Build the image for the indexer:

```sh
docker build -t fossil-indexer -f Dockerfile.indexer .
```

You should be able to run the image after build is successful:

```sh
docker run fossil-indexer
```

<p  align="right">(<a  href="#readme-top">back to top</a>)</p>

## Debugging

### Prerequisites

Before starting debugging, ensure you have:

- Rust and Cargo installed
- Docker and Docker Compose
- PostgreSQL client (`psql`)
- VS Code with Rust extensions

### Installing PostgreSQL Client

Install the PostgreSQL client (psql) for your operating system:

**macOS**:

```bash
# Using Homebrew
brew install libpq

# For Apple Silicon Macs (M1, M2, etc.)
echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc

# For Intel Macs
# echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc

# Apply changes
source ~/.zshrc

# Optional: For building applications that link against libpq
# For Apple Silicon Macs:
export LDFLAGS="-L/opt/homebrew/opt/libpq/lib"
export CPPFLAGS="-I/opt/homebrew/opt/libpq/include"
# For Intel Macs:
# export LDFLAGS="-L/usr/local/opt/libpq/lib"
# export CPPFLAGS="-I/usr/local/opt/libpq/include"

# Alternative: Using Postgres.app
# 1. Download from https://postgresapp.com/
# 2. Move to Applications folder
# 3. Add to PATH:
echo 'export PATH="/Applications/Postgres.app/Contents/Versions/latest/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

**Linux (Ubuntu/Debian)**:

```bash
sudo apt update
sudo apt install postgresql-client
```

**Linux (Fedora/RHEL)**:

```bash
sudo dnf install postgresql
```

**Windows**:

1. Download the PostgreSQL installer from [postgresql.org](https://www.postgresql.org/download/windows/)
2. Run the installer and select only "Command Line Tools"
3. Add to PATH: `C:\Program Files\PostgreSQL\<version>\bin`

Verify installation:

```bash
psql --version
```

### Launch Steps

1. **Start the Database**:

```bash
# Launch PostgreSQL container
docker-compose -f docker-compose.local.yml up -d

# Verify database is running
docker ps
psql postgresql://postgres:postgres@localhost:5432/postgres -c "SELECT 1"
```

2. **Configure Environment**:

```bash
# Create debug configuration
cat > .env << EOL
DB_CONNECTION_STRING=postgresql://postgres:postgres@localhost:5432/postgres
NODE_CONNECTION_STRING=<your_ethereum_node_rpc_url>
ROUTER_ENDPOINT=0.0.0.0:3000
RUST_LOG=debug
EOL
```

3. **Setup VS Code Debugging**:

First, install the CodeLLDB extension:

```bash
# Install CodeLLDB extension in VS Code
code --install-extension vadimcn.vscode-lldb
```

Or manually:

- Open VS Code
- Go to Extensions (Cmd+Shift+X)
- Search for "CodeLLDB"
- Install the extension by Vadim Chugunov

Then create the launch configuration:

```bash
# Create launch configuration
mkdir -p .vscode
cat > .vscode/launch.json << EOL
{
    "version": "0.2.0",
    "configurations": [
        {
            "type": "lldb",
            "request": "launch",
            "name": "Debug Fix Mode",
            "cargo": {
                "args": ["build"]
            },
            "args": ["fix", "--start", "0", "--end", "1000"],
            "cwd": "\${workspaceFolder}"
        },
        {
            "type": "lldb",
            "request": "launch",
            "name": "Debug Update Mode",
            "cargo": {
                "args": ["build"]
            },
            "args": ["update", "--start", "0", "--loopsize", "10"],
            "cwd": "\${workspaceFolder}"
        }
    ]
}
EOL
```

### Debug Process

1. **Set Breakpoints** in VS Code:

   - `src/commands/mod.rs`: `process_block`, `update_blocks`, `fill_gaps`
   - `src/db/mod.rs`: `write_blockheader`, `find_first_gap`

2. **Launch Debug Session**:

   - Open VS Code command palette (Cmd+Shift+P)
   - Select "Debug: Start Debugging" or press F5
   - Choose "Debug Fix Mode" or "Debug Update Mode"

3. **Monitor While Debugging**:

```bash
# Watch database
psql postgresql://postgres:postgres@localhost:5432/postgres -c "\
    SELECT number, parent_hash, timestamp \
    FROM blockheaders ORDER BY number DESC LIMIT 5;"

# Check application logs
RUST_LOG=debug cargo run -- fix --start 0 --end 1000
```

### Troubleshooting

If you encounter issues:

1. **Database Connection**:

```bash
# Check database status
docker logs fossil-headers-db-db-1
docker-compose -f docker-compose.local.yml ps
```

2. **Application Errors**:

```bash
# Run with trace logging
RUST_LOG=trace cargo run -- fix --start 0 --end 10

# Check database connection
psql postgresql://postgres:postgres@localhost:5432/postgres
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- Endpoints -->

## Endpoints

### Health

```
GET /
```

Used to ping server for alive status.

#### Request:

```c
curl --location '<ROUTER_ENDPOINT>'
--header 'Content-Type: application/json'
```

#### Response:

```c
Healthy
```
