#!/usr/bin/env python3
"""
Multi-Port ACARS Message Processor

This script listens on multiple UDP ports for ACARS messages from different Jaero instances,
processes them, and splits them into separate files based on their message labels.
"""

import asyncio
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

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
            config['output_dir'] = 'acars_by_label'
        if 'buffer_timeout' not in config:
            config['buffer_timeout'] = 60
        if 'ports' not in config or not config['ports']:
            raise ValueError("No UDP ports specified in the configuration file")
            
        return config
    
    def parse_acars_message(self, message: str) -> Tuple[Optional[str], str]:
        """
        Parse an ACARS message and extract its label.
        
        Args:
            message: A complete ACARS message
            
        Returns:
            tuple: (label, message) or (None, message) if no label found
        """
        # Extract the first line of the message (header line)
        first_line = message.split('\n')[0].strip()
        
        # The format appears to be consistent with fields separated by spaces
        # After the timestamp, metadata, and aircraft ID, we have delimiter, label, etc.
        # Example: 00:16:25 18-03-25 UTC AES:E4920F GES:D0 2 ..PTZNG 2 52 A
        parts = first_line.split()
        
        # We need at least 9 parts to have a label
        if len(parts) >= 9:
            # The label should be at a specific position (9th item)
            label_position = 8  # 0-indexed, so this is the 9th item
            if label_position < len(parts):
                label = parts[label_position]
                # Verify it's a valid label (alphanumeric)
                if re.match(r'^[A-Za-z0-9]+$', label):
                    return label, message
        
        return None, message
    
    def append_message_to_file(self, label: Optional[str], message: str) -> None:
        """
        Append a message to the appropriate file based on its label.
        
        Args:
            label: Message label or None for unlabeled messages
            message: The ACARS message to append
        """
        if label:
            output_file = os.path.join(self.config['output_dir'], f"acars_{label}.txt")
        else:
            output_file = os.path.join(self.config['output_dir'], "acars_unlabeled.txt")
        
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
            
            # Process the message
            label, full_message = self.parse_acars_message(message)
            self.append_message_to_file(label, full_message)
            print(f"Port {port}: Processed message with label: {label or 'None'}")
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
                        label, full_message = self.parse_acars_message(message)
                        self.append_message_to_file(label, full_message)
                        print(f"Port {port}: Processed timeout message with label: {label or 'None'}")
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
