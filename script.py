import os
import json
import time
import logging
from typing import Dict, Any, Optional, List

import requests
from web3 import Web3
from web3.contract import Contract
from web3.exceptions import BlockNotFound
from web3.middleware import geth_poa_middleware
from requests.exceptions import RequestException

# --- Basic Configuration Setup ---
# In a real-world application, this would be loaded from environment variables
# or a secure configuration management system.
CONFIG = {
    "SOURCE_CHAIN_RPC_URL": "https://rpc.sepolia.org",
    "DESTINATION_CHAIN_RPC_URL": "https://rpc.goerli.mudit.blog",
    "LISTENER_PRIVATE_KEY": "0x0000000000000000000000000000000000000000000000000000000000000001", # DANGER: For demonstration only. NEVER hardcode private keys.
    "SOURCE_BRIDGE_CONTRACT_ADDRESS": "0x5a18512142d1767C1F2815347209941940a63931", # Example address
    "DESTINATION_BRIDGE_CONTRACT_ADDRESS": "0xbC485295491102C465f2471649A1F441991B3f47", # Example address
    "API_VALIDATION_ENDPOINT": "https://api.example.com/validate_tx",
    "STATE_FILE_PATH": "./listener_state.json",
    "BLOCK_PROCESSING_INTERVAL_SECONDS": 15,
    "CONFIRMATION_BLOCKS": 6,
}

# --- Contract ABIs (Simplified for demonstration) ---
# In a real project, these would be loaded from dedicated ABI files.
SOURCE_BRIDGE_ABI = json.loads('''
[
    {
        "anonymous": false,
        "inputs": [
            {"indexed": true, "name": "token", "type": "address"},
            {"indexed": true, "name": "sender", "type": "address"},
            {"indexed": true, "name": "recipient", "type": "address"},
            {"indexed": false, "name": "amount", "type": "uint256"},
            {"indexed": false, "name": "destinationChainId", "type": "uint256"},
            {"indexed": false, "name": "nonce", "type": "uint256"}
        ],
        "name": "TokensLocked",
        "type": "event"
    }
]
''')

DESTINATION_BRIDGE_ABI = json.loads('''
[
    {
        "constant": false,
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "recipient", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "sourceNonce", "type": "uint256"}
        ],
        "name": "mintTokens",
        "outputs": [],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    }
]
''')

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class StateManager:
    """Manages the persistent state of the listener, such as the last processed block."""

    def __init__(self, file_path: str):
        """
        Initializes the StateManager.

        Args:
            file_path (str): The path to the JSON file where state is stored.
        """
        self.file_path = file_path
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Loads state from the file, or returns a default state if the file doesn't exist."""
        if not os.path.exists(self.file_path):
            logging.warning(f"State file not found at {self.file_path}. Initializing with default state.")
            return {"last_processed_block": None}
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.error(f"Failed to load or parse state file: {e}. Starting with a default state.")
            return {"last_processed_block": None}

    def get_last_processed_block(self) -> Optional[int]:
        """Returns the last block number that was successfully processed."""
        return self.state.get("last_processed_block")

    def update_last_processed_block(self, block_number: int):
        """
        Updates the last processed block number in the state and saves it to the file.

        Args:
            block_number (int): The new block number to save.
        """
        self.state["last_processed_block"] = block_number
        self._save_state()

    def _save_state(self):
        """Saves the current state to the JSON file."""
        try:
            with open(self.file_path, 'w') as f:
                json.dump(self.state, f, indent=4)
            logging.info(f"State successfully saved. Last processed block: {self.state['last_processed_block']}")
        except IOError as e:
            logging.error(f"Could not write to state file {self.file_path}: {e}")

class ChainConnector:
    """
    Handles connection to a single blockchain via Web3 and provides contract interaction utilities.
    This class abstracts away the complexities of retries and network configurations.
    """
    
    def __init__(self, rpc_url: str, private_key: Optional[str] = None):
        """
        Initializes the connection to a blockchain node.

        Args:
            rpc_url (str): The HTTP RPC endpoint of the blockchain node.
            private_key (Optional[str]): The private key for signing transactions. Required for write operations.
        """
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        # Inject middleware for PoA chains like Goerli, Sepolia, etc.
        self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        if not self.web3.is_connected():
            raise ConnectionError(f"Failed to connect to blockchain node at {rpc_url}")
        
        self.account = None
        if private_key:
            self.account = self.web3.eth.account.from_key(private_key)
        
        logging.info(f"Successfully connected to node at {rpc_url}. Chain ID: {self.web3.eth.chain_id}")
        if self.account:
            logging.info(f"Signer address: {self.account.address}")

    def get_contract(self, address: str, abi: List[Dict]) -> Contract:
        """Returns a Web3 contract instance."""
        checksum_address = Web3.to_checksum_address(address)
        return self.web3.eth.contract(address=checksum_address, abi=abi)

class EventProcessor:
    """
    Processes events from the source chain and triggers actions on the destination chain.
    This class encapsulates the business logic of the bridge.
    """

    def __init__(self, source_chain: ChainConnector, dest_chain: ChainConnector, config: Dict[str, Any]):
        """
        Initializes the EventProcessor.

        Args:
            source_chain (ChainConnector): Connector for the source blockchain.
            dest_chain (ChainConnector): Connector for the destination blockchain.
            config (Dict[str, Any]): The application configuration dictionary.
        """
        self.source_chain = source_chain
        self.dest_chain = dest_chain
        self.config = config
        self.dest_contract = self.dest_chain.get_contract(
            self.config["DESTINATION_BRIDGE_CONTRACT_ADDRESS"],
            DESTINATION_BRIDGE_ABI
        )

    def process_event(self, event: Dict[str, Any]):
        """Processes a single 'TokensLocked' event."""
        log_index = event.get('logIndex', 'N/A')
        tx_hash = event.get('transactionHash', b'').hex()
        logging.info(f"Processing event from transaction {tx_hash} at log index {log_index}")

        # 1. Parse and validate the event data
        event_args = event.get('args')
        if not self._is_event_valid(event_args):
            logging.warning(f"Skipping invalid or incomplete event: {event_args}")
            return

        # 2. Perform external validation (simulated API call)
        if not self._external_validation(tx_hash, event_args['sender']):
            logging.warning(f"External validation failed for tx {tx_hash}. Skipping.")
            return

        # 3. Trigger the minting transaction on the destination chain
        self._execute_mint_transaction(event_args)

    def _is_event_valid(self, args: Optional[Dict[str, Any]]) -> bool:
        """Performs basic validation on the event arguments."""
        if not args:
            return False
        required_keys = ["token", "recipient", "amount", "nonce"]
        return all(key in args for key in required_keys) and args['amount'] > 0

    def _external_validation(self, tx_hash: str, sender_address: str) -> bool:
        """
        Simulates a call to an external API for secondary validation, e.g., fraud detection.
        """
        try:
            payload = {"txHash": tx_hash, "sender": sender_address}
            response = requests.post(self.config["API_VALIDATION_ENDPOINT"], json=payload, timeout=10)
            
            # This is a mock: in a real case, we'd check the response content.
            # Here, we simulate success unless the server gives an error.
            response.raise_for_status()
            logging.info(f"External validation successful for tx {tx_hash}")
            return True
        except RequestException as e:
            # In a real scenario, we might retry or queue the event for later.
            logging.error(f"External API validation failed for tx {tx_hash}: {e}. Mocking success for demo purposes.")
            # For this simulation, we'll allow it to proceed even if the mock API fails.
            return True

    def _execute_mint_transaction(self, event_args: Dict[str, Any]):
        """Builds, signs, and sends the minting transaction to the destination chain."""
        if not self.dest_chain.account:
            logging.error("Cannot execute mint transaction: destination chain connector has no private key.")
            return

        try:
            recipient = Web3.to_checksum_address(event_args['recipient'])
            token = Web3.to_checksum_address(event_args['token'])
            amount = event_args['amount']
            source_nonce = event_args['nonce']

            logging.info(f"Preparing to mint {amount} of token {token} for {recipient} with source nonce {source_nonce}.")

            tx_params = {
                'from': self.dest_chain.account.address,
                'nonce': self.dest_chain.web3.eth.get_transaction_count(self.dest_chain.account.address),
                'gasPrice': self.dest_chain.web3.eth.gas_price,
            }
            
            mint_tx = self.dest_contract.functions.mintTokens(
                token,
                recipient,
                amount,
                source_nonce
            ).build_transaction(tx_params)

            signed_tx = self.dest_chain.web3.eth.account.sign_transaction(mint_tx, self.dest_chain.account.key)
            tx_hash = self.dest_chain.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            logging.info(f"Mint transaction sent to destination chain. Tx Hash: {tx_hash.hex()}")
            
            # In a production system, you would wait for the transaction receipt and handle failures.
            # tx_receipt = self.dest_chain.web3.eth.wait_for_transaction_receipt(tx_hash)
            # logging.info(f"Mint transaction confirmed. Gas used: {tx_receipt['gasUsed']}")

        except Exception as e:
            logging.error(f"Failed to execute mint transaction for source nonce {source_nonce}: {e}")

class BridgeEventListener:
    """
    The main orchestrator class that listens for events on a source chain
    and coordinates processing them.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the main listener service.

        Args:
            config (Dict[str, Any]): The application configuration.
        """
        self.config = config
        self.state_manager = StateManager(config["STATE_FILE_PATH"])
        
        self.source_chain = ChainConnector(config["SOURCE_CHAIN_RPC_URL"])
        self.dest_chain = ChainConnector(config["DESTINATION_CHAIN_RPC_URL"], config["LISTENER_PRIVATE_KEY"])
        
        self.source_contract = self.source_chain.get_contract(
            config["SOURCE_BRIDGE_CONTRACT_ADDRESS"], 
            SOURCE_BRIDGE_ABI
        )
        self.event_processor = EventProcessor(self.source_chain, self.dest_chain, config)
        self.running = True

    def _get_start_block(self) -> int:
        """Determines the block number to start scanning from."""
        last_processed = self.state_manager.get_last_processed_block()
        if last_processed:
            logging.info(f"Resuming from last processed block: {last_processed}")
            return last_processed + 1
        else:
            # If no state, start from a recent block to avoid scanning the whole chain history
            latest_block = self.source_chain.web3.eth.block_number
            start_block = latest_block - 100 # Look back 100 blocks on first run
            logging.info(f"No previous state found. Starting from block {start_block}")
            return max(start_block, 0)

    def run(self):
        """Starts the main event listening loop."""
        logging.info("--- Cross-Chain Bridge Event Listener Starting ---")
        start_block = self._get_start_block()

        while self.running:
            try:
                latest_block = self.source_chain.web3.eth.block_number
                # Process up to a block with sufficient confirmations
                to_block = latest_block - self.config.get("CONFIRMATION_BLOCKS", 6)

                if start_block > to_block:
                    logging.info(f"Waiting for new blocks to be confirmed. Current head: {latest_block}")
                    time.sleep(self.config["BLOCK_PROCESSING_INTERVAL_SECONDS"])
                    continue

                logging.info(f"Scanning for 'TokensLocked' events from block {start_block} to {to_block}...")

                event_filter = self.source_contract.events.TokensLocked.create_filter(
                    fromBlock=start_block,
                    toBlock=to_block
                )
                events = event_filter.get_all_entries()

                if events:
                    logging.info(f"Found {len(events)} new event(s).")
                    for event in events:
                        self.event_processor.process_event(event)
                else:
                    logging.info("No new events found in this range.")

                # Update state and prepare for the next iteration
                self.state_manager.update_last_processed_block(to_block)
                start_block = to_block + 1

                time.sleep(self.config["BLOCK_PROCESSING_INTERVAL_SECONDS"])

            except BlockNotFound:
                logging.warning("Block range not found, possibly due to a reorg. Adjusting scan range.")
                # Simple reorg handling: step back a few blocks and retry
                start_block = self.source_chain.web3.eth.block_number - self.config.get("CONFIRMATION_BLOCKS", 6) - 10
                time.sleep(self.config["BLOCK_PROCESSING_INTERVAL_SECONDS"])
            except Exception as e:
                logging.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
                logging.info("Retrying after a short delay...")
                time.sleep(30) # Wait longer after a critical error

    def stop(self):
        """Stops the event listening loop gracefully."""
        logging.info("--- Shutting down listener --- ")
        self.running = False

if __name__ == "__main__":
    listener = BridgeEventListener(CONFIG)
    try:
        listener.run()
    except KeyboardInterrupt:
        listener.stop()
        logging.info("Listener stopped by user.")
