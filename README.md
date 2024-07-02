# Ethereum NFT Tracker

This Python script retrieves the latest block from the Ethereum blockchain, extracts all wallet addresses involved in transactions within that block, checks for NFTs held by these addresses, and outputs the results. The script uses the Etherscan API for interacting with the Ethereum blockchain.

## Features

- Retrieve the latest Ethereum block.
- Extract wallet addresses involved in transactions within the block.
- Check for NFTs held by these addresses.
- Save NFT contract addresses to a text file.
- Gracefully stop the script using keyboard interrupt (`Ctrl+C`).

## Requirements

- Python 3.x
- `requests` library

## Installation

1. Clone this repository:

```sh
git clone https://github.com/layerist/ethereum-nft-tracker.git
cd ethereum-nft-tracker
```

2. Install the required Python packages:

```sh
pip install requests
```

3. Obtain an Etherscan API key from [Etherscan.io](https://etherscan.io/register).

## Configuration

1. Open the script file (`eth_nft_tracker.py`).
2. Replace `'YOUR_ETHERSCAN_API_KEY'` with your actual Etherscan API key:

```python
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
```

## Usage

Run the script:

```sh
python eth_nft_tracker.py
```

The script will:

1. Retrieve the latest Ethereum block.
2. Extract wallet addresses involved in transactions within that block.
3. Check for NFTs held by these addresses.
4. Output the NFT contract addresses and save them to `nft_addresses.txt`.

### Stopping the Script

You can stop the script gracefully by pressing `Ctrl+C`.

## Example

After running the script, the output will be similar to:

```
Latest Block: 12345678
Addresses in Block: {'0xabc...', '0xdef...', '0xghi...'}
NFT Addresses: {'0x123...', '0x456...', '0x789...'}
```

The NFT contract addresses will also be saved to `nft_addresses.txt`:

```
0x123...
0x456...
0x789...
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
```

### Creating the Script File

Save the provided script in a file named `eth_nft_tracker.py`.

### Creating the GitHub Repository

1. Go to GitHub and create a new repository named `ethereum-nft-tracker`.
2. Clone the repository to your local machine:

```sh
git clone https://github.com/layerist/ethereum-nft-tracker.git
cd ethereum-nft-tracker
```

3. Add the script and README file to the repository:

```sh
touch eth_nft_tracker.py
# Paste the script content into eth_nft_tracker.py
touch README.md
# Paste the README content into README.md
```

4. Commit and push the changes:

```sh
git add eth_nft_tracker.py README.md
git commit -m "Initial commit"
git push origin main
```
