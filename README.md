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

## Getting Started

To get this application runninng locally, follow these steps.

### Prerequisites

What you would need:

- Rust

```
https://www.rust-lang.org/tools/install
```

### Installation

1. Clone the repo

```sh
git clone https://github.com/OilerNetwork/fossil-headers-db.git
```

1. Create a .env file in the project's root folder

_fossil-headers-db/.env_

```
DB_CONNECTION_STRING=<db_connection_string>
NODE_CONNECTION_STRING=<node_connection_string>
ROUTER_ENDPOINT=<router_endpoint_string>
RUST_LOG=<log_level> [optional]
```

1. Build project

```sh
cargo build
```

<p  align="right">(<a  href="#readme-top">back to top</a>)</p>
  
<!-- USAGE EXAMPLES -->

## Usage

### Mode 1 - Update

Fetches blockheaders and transaction data via RPC and writes to DB.

**Usage:** _cargo run update_

**Optional parameters:**

1.  _start <block_number>_

- First block number to start updating the database from. (Inclusive)

- **Default**: Latest block in the database + 1

1.  _end <block_number>_

- Last block number to update the database to. (Inclusive)

- **Default**: Polling mode - updates to latest block, after which it polls for new blocks

2.  _loopsize <num_threads>_

- Max number of threads running at once

- **Default**: Max functional connections for our DB -- 4000
  **Examples:**

```sh
cargo  run  update
```

```sh
cargo  run  update  --loopsize  10
```

```sh
cargo  run  update  --start  19983846
```

```sh
cargo  run  update  --end  19983849
```

```sh
cargo  run  update  -s  19983846  -e  19983849
```

```sh
cargo  run  update  -s  19983846  -end  19983849  -l  100
```

### Mode 2 - Fix

Patches missing blockheaders and transaction data from the DB, retrieving via RPC

**Usage:** _cargo run update_

**Optional parameters:**

1.  _start <block_number>_

- First block number to start checking the database from. (Inclusive)

- **Default**: 0

1.  _end <block_number>_

- Last block number to check the database to. (Inclusive)

- **Default**: Last entry in the database

**Examples:**

```sh
cargo  run  fix
```

```sh
cargo  run  fix  --start  19983846
```

```sh
cargo  run  fix  --end  19983849
```

```sh
cargo  run  fix  -s  19983846  -e  19983849
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

# Endpoints

## General

### 1. Health

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

## MMR

### 1. GET latest updated MMR information

Retrieves the latest MMR state

### Request:

```c
curl --location '127.0.0.1:8080/mmr'
--header 'Content-Type: application/json'
```

### Response:

```c
{
"latest_blocknumber": 17992
"latest_roothash": "0x02e6baea3eba34b9c581bd719465a2181c5dc989891517add951ffb5b0d421f0",
"update_timestamp": "2024-08-02T05:24:06.928467Z"
}
```

### 2. GET proof

Retrieve proof for the provided `<blocknumber>`

### Request:

```c
curl --location '127.0.0.1:8080/mmr/<blocknumber>'
--header 'Content-Type: application/json'
```

### Response:

```c
{
"peaks_hashes": "[\"0x5a0a7e39c749e1c03feaff0a6d8fca6181253d47c9aae3c6ddd0c0475a9c8a61\",\"0x04182373152d407f88a71a38960aa714e2b3a8988e3e3606e2ced21473e4a0c5\",\"0x2558950083d01e2a8a6699e44e3d97642586b3bde1d237a9d34c8d9d77178a22\",\"0x6a29cea580488bd512185f9cc16deec823dd58b85f1dd8408cc46c40f8ab10b8\",\"0x0d995a2bf7801bc7e56776bfc441b957d7959d662c5262405feb572be1928011\",\"0x1933e13bd7e5e4227b20a4f972a302eaad8da1fc0f455138d79af68da3c86e3f\",\"0x0a9b742073888cbd44e0b609aed502ccc8454d32b827e86f66181d6a7fa4a086\",\"0x7bef241cbe2dbbc2c04eb9431b2471b2678bc792bf460ec9d8b15882301f9da7\",\"0x8bd6d00cfe79edc41a17c724d965427d41ab865d58ce57690316ce368386080a\"]",
"element_index": "2",
"elements_count": "57113",
"element_hash": "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
"sibling_hashes": "[\"0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3\",\"0xabbd3abf1b19fd233c45ea4a2051369190b069f58f9c5fe8d016c461df048748\",\"0xa2ce5dd7d1a450b2e3ef0b41092b5a782d3090ac5d7f5257d82eb0ca67ba21ec\",\"0x09b3664e5f2495f402b45ae8b056a4e9c6beeafd44494aebcc671c0a337322ab\",\"0x84a1dee387cded87e6bee1382df8b40b363dd8212c96b18dff77da2df504db38\",\"0x8d970b7ad08cfc4ab69b323d941e80478a4c4fc99ee165c4e6525d3a1e46cda7\",\"0x5cba303c0aaf9f6bb48a7003bae08ae19b44732dc5d0366a127bdf4ee82c0d21\",\"0x520559e1f475be63069c022f59b99e9272dbbd8d2888635055d2b516539c8426\",\"0x6cb0b6aa14349728919cb59ccd3c0bb62721539202f4d5c42bfba1612a577de7\",\"0x2bf3c9d3a66d63d4b16e8ce7681ccbe7e382ef721001dac950088cfd2a3dfe5d\",\"0x66ba3f44720909e4ed0ba754efb13a1eb85fe6a1c80770441cc36d762108ee60\",\"0x3661b2303a69a0a960c4bb4e732daa56f44a57f95700e20ad1915866d18e1980\",\"0x044c1636b3a10f042b40ee3ee4ccd0156ddebd88d4daab1d2c68ed6d9918fa4c\",\"0xdee2ec7f050d0c19467f33e26363bfd869d5fa3847fd49bd1bb218469e92c7af\"]"
}
```

<p  align="right">(<a  href="#readme-top">back to top</a>)</p>
