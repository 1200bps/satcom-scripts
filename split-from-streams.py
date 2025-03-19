#!/usr/bin/env python3
"""
Multi-Port ACARS Message Processor

This script listens on multiple UDP ports for ACARS messages from different Jaero instances,
processes them, and splits them into separate files based on various message criteria.
"""

import asyncio
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple, Any

class AcarsProtocol(asyncio.DatagramProtocol):
    """Protocol for handling UDP datagrams containing ACARS messages."""
    
    def __init__(self, processor, port):
        """
        Initialize the protocol.
        
        Args:
            processor: The AcarsProcessor instance
            port: The UDP port this protocol is listening on
        """
        self.processor = processor
        self.port = port
        
    def datagram_received(self, data, addr):
        """
        Called when a datagram is received.
        
        Args:
            data: The received data
            addr: The sender's address
        """
        try:
            decoded_data = data.decode('utf-8')
            self.processor.add_data_to_buffer(self.port, decoded_data)
        except UnicodeDecodeError:
            print(f"Warning: Received data on port {self.port} that could not be decoded as UTF-8", file=sys.stderr)

class AcarsProcessor:
    """Processes ACARS messages from multiple UDP ports."""
    
    def __init__(self, config_file: str):
        """
        Initialize the processor.
        
        Args:
            config_file: Path to the JSON configuration file
        """
        self.config = self._load_config(config_file)
        self.buffers: Dict[int, str] = {}  # Port -> buffer
        self.last_process_times: Dict[int, float] = {}  # Port -> timestamp
        self.transport_handles: Dict[int, asyncio.DatagramTransport] = {}  # Port -> transport
        
    def _load_config(self, config_file: str) -> dict:
        """
        Load the configuration from a JSON file.
        
        Args:
            config_file: Path to the JSON configuration file
            
        Returns:
            dict: The loaded configuration
            
        Raises:
            ValueError: If no UDP ports are specified in the configuration
        """
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Set defaults if not specified
        if 'host' not in config:
            config['host'] = '127.0.0.1'
        if 'output_dir' not in config:
            config['output_dir'] = 'acars_split'
        if 'buffer_timeout' not in config:
            config['buffer_timeout'] = 60
        if 'split_by' not in config:
            config['split_by'] = 'label'  # Default to label
        if 'ports' not in config or not config['ports']:
            raise ValueError("No UDP ports specified in the configuration file")
            
        # Validate split_by
        valid_split_methods = ['label', 'tail', 'type', 'keyword']
        if config['split_by'] not in valid_split_methods:
            print(f"Warning: Invalid split_by value: '{config['split_by']}'. Using 'label' instead.", file=sys.stderr)
            config['split_by'] = 'label'
            
        # Ensure keyword is set if splitting by keyword
        if config['split_by'] == 'keyword' and ('keyword' not in config or not config['keyword']):
            print(f"Warning: split_by is 'keyword' but no keyword specified. Using 'label' instead.", file=sys.stderr)
            config['split_by'] = 'label'
            
        return config
    
    def extract_tail_number(self, message: str) -> Optional[str]:
        """
        Extract the tail number from an ACARS message.
        
        Args:
            message: A complete ACARS message
            
        Returns:
            str: Tail number or None if not found
        """
        match = re.search(r'AES:[A-F0-9]+\s+GES:[A-Z0-9]+\s+\d+\s+(\.?[A-Za-z0-9-]+)', message)
        if match:
            return match.group(1).strip('.')
        return None
    
    def determine_message_type(self, message: str) -> str:
        """
        Determine if a message is CPDLC, ADS-C, MIAM or other.
        
        Args:
            message: A complete ACARS message
            
        Returns:
            str: Message type ('CPDLC', 'ADS-C', 'MIAM', or 'OTHER')
        """
        if "FANS-1/A CPDLC" in message:
            return "CPDLC"
        elif "ADS-C" in message:
            return "ADS-C"
        elif "MIAM" in message:
            return "MIAM"
        else:
            return "OTHER"
    
    def contains_keyword(self, message: str, keyword: str) -> bool:
        """
        Check if a message contains a specific keyword (case-insensitive).
        
        Args:
            message: A complete ACARS message
            keyword: Keyword to search for
            
        Returns:
            bool: True if the message contains the keyword, False otherwise
        """
        return keyword.lower() in message.lower()
    
    def extract_message_label(self, message: str) -> Optional[str]:
        """
        Extract the message label from an ACARS message.
        
        Args:
            message: A complete ACARS message
            
        Returns:
            str: Message label or None if not found
        """
        match = re.search(r'!\s+([A-Za-z0-9]{2})\s+[A-Za-z0-9]', message)
        if match:
            return match.group(1)
        return None
    
    def get_split_key(self, message: str) -> Optional[str]:
        """
        Get the appropriate key for splitting based on the configuration.
        
        Args:
            message: A complete ACARS message
            
        Returns:
            str: The key to split by or None if no key could be determined
        """
        split_by = self.config['split_by']
        
        if split_by == 'label':
            return self.extract_message_label(message)
        elif split_by == 'tail':
            return self.extract_tail_number(message)
        elif split_by == 'type':
            return self.determine_message_type(message)
        elif split_by == 'keyword':
            keyword = self.config['keyword']
            if self.contains_keyword(message, keyword):
                return f"containing_{keyword}"
            else:
                return f"not_containing_{keyword}"
        
        return None
    
    def append_message_to_file(self, key: Optional[str], message: str) -> None:
        """
        Append a message to the appropriate file based on its split key.
        
        Args:
            key: The key to split by or None for unclassified messages
            message: The ACARS message to append
        """
        split_by = self.config['split_by']
        
        if key:
            if split_by == 'label':
                output_file = os.path.join(self.config['output_dir'], f"acars_label_{key}.txt")
            elif split_by == 'tail':
                output_file = os.path.join(self.config['output_dir'], f"acars_tail_{key}.txt")
            elif split_by == 'type':
                output_file = os.path.join(self.config['output_dir'], f"acars_type_{key}.txt")
            elif split_by == 'keyword':
                output_file = os.path.join(self.config['output_dir'], f"acars_{key}.txt")
            else:
                output_file = os.path.join(self.config['output_dir'], f"acars_unclassified.txt")
        else:
            output_file = os.path.join(self.config['output_dir'], "acars_unclassified.txt")
        
        # Check if the file exists and has content
        file_exists = os.path.exists(output_file)
        file_has_content = file_exists and os.path.getsize(output_file) > 0
        
        # Append the message to the file
        with open(output_file, 'a') as file:
            if file_has_content:
                # Add two newlines between messages
                file.write('\n\n')
            file.write(message)
    
    def process_buffer(self, port: int) -> int:
        """
        Process the buffer for a specific port to extract and handle complete messages.
        
        Args:
            port: The UDP port whose buffer to process
            
        Returns:
            int: Number of messages processed
        """
        buffer = self.buffers[port]
        timestamp_pattern = r'^\d{2}:\d{2}:\d{2} \d{2}-\d{2}-\d{2} UTC'
        matches = list(re.finditer(timestamp_pattern, buffer, re.MULTILINE))
        
        if len(matches) < 2:
            # Not enough timestamps to identify a complete message
            return 0
        
        messages_processed = 0
        
        # Process all complete messages (except possibly the last one)
        for i in range(len(matches) - 1):
            start_pos = matches[i].start()
            end_pos = matches[i+1].start()
            
            # Extract the complete message
            message = buffer[start_pos:end_pos].strip()
            
            # Get the key for splitting
            split_key = self.get_split_key(message)
            
            # Append the message to the appropriate file
            self.append_message_to_file(split_key, message)
            
            split_by = self.config['split_by']
            split_name = split_key if split_key else "unclassified"
            print(f"Port {port}: Processed message with {split_by}: {split_name}")
            
            messages_processed += 1
        
        # Keep the last (potentially incomplete) message in the buffer
        self.buffers[port] = buffer[matches[-1].start():]
        
        return messages_processed
    
    def add_data_to_buffer(self, port: int, data: str) -> None:
        """
        Add received data to the buffer for a specific port and process it.
        
        Args:
            port: The UDP port that received the data
            data: The data to add to the buffer
        """
        self.buffers[port] += data
        
        # Process the buffer
        messages_processed = self.process_buffer(port)
        
        if messages_processed > 0:
            self.last_process_times[port] = time.time()
    
    async def check_timeouts(self) -> None:
        """
        Periodically check buffers for timeout conditions and process incomplete messages.
        """
        buffer_timeout = self.config['buffer_timeout']
        
        while True:
            await asyncio.sleep(buffer_timeout)
            
            current_time = time.time()
            for port in self.buffers:
                # If it's been a while since we processed anything and we have data,
                # try to process what we have even if it might be incomplete
                if (self.buffers[port] and 
                    (current_time - self.last_process_times[port] > buffer_timeout * 2)):
                    
                    timestamp_pattern = r'^\d{2}:\d{2}:\d{2} \d{2}-\d{2}-\d{2} UTC'
                    match = re.search(timestamp_pattern, self.buffers[port], re.MULTILINE)
                    
                    if match:
                        # We have at least one timestamp, try to process it as a message
                        message = self.buffers[port][match.start():].strip()
                        split_key = self.get_split_key(message)
                        self.append_message_to_file(split_key, message)
                        
                        split_by = self.config['split_by']
                        split_name = split_key if split_key else "unclassified"
                        print(f"Port {port}: Processed timeout message with {split_by}: {split_name}")
                        
                        self.buffers[port] = ""
                        self.last_process_times[port] = current_time
    
    async def setup_listener(self, port: int) -> None:
        """
        Set up a UDP listener for a specific port.
        
        Args:
            port: The UDP port to listen on
        """
        loop = asyncio.get_running_loop()
        
        # Create a datagram endpoint (UDP socket)
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: AcarsProtocol(self, port),
            local_addr=(self.config['host'], port)
        )
        
        self.transport_handles[port] = transport
        print(f"Listening for ACARS messages on {self.config['host']}:{port}...")
    
    async def run(self) -> None:
        """
        Start the ACARS processor and listen on all configured ports.
        """
        # Create the output directory
        os.makedirs(self.config['output_dir'], exist_ok=True)
        
        # Print configuration information
        print(f"ACARS Message Processor Configuration:")
        print(f"  Host: {self.config['host']}")
        print(f"  Ports: {', '.join(map(str, self.config['ports']))}")
        print(f"  Output Directory: {self.config['output_dir']}")
        print(f"  Split by: {self.config['split_by']}")
        if self.config['split_by'] == 'keyword':
            print(f"  Keyword: {self.config['keyword']}")
        print(f"  Buffer Timeout: {self.config['buffer_timeout']} seconds")
        
        # Initialize buffers and last process times
        for port in self.config['ports']:
            self.buffers[port] = ""
            self.last_process_times[port] = time.time()
        
        # Set up listeners for all ports
        setup_tasks = []
        for port in self.config['ports']:
            setup_tasks.append(self.setup_listener(port))
        
        await asyncio.gather(*setup_tasks)
        
        # Start the timeout checker
        timeout_checker = asyncio.create_task(self.check_timeouts())
        
        try:
            # Run forever
            await asyncio.Future()
        finally:
            # Clean up
            timeout_checker.cancel()
            for transport in self.transport_handles.values():
                transport.close()

def main():
    """Main entry point of the script."""
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <config_file>")
        sys.exit(1)
        
    config_file = sys.argv[1]
    processor = AcarsProcessor(config_file)
    
    try:
        asyncio.run(processor.run())
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()
