import os
import re
import argparse
import sys

def parse_acars_log(log_file_path, output_dir):
    """
    Parse ACARS log file and split messages by label into separate files.
    
    Args:
        log_file_path (str): Path to the ACARS log file
        output_dir (str): Directory to store the output files
    """
    # Validate input file
    if not os.path.isfile(log_file_path):
        print(f"Usage: splitter.py input_file -o output_dir", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Read the log file
        with open(log_file_path, 'r') as file:
            content = file.read()
        
        # Split content into messages
        # A message starts with a timestamp line and continues until the next timestamp line
        timestamp_pattern = r'^\d{2}:\d{2}:\d{2} \d{2}-\d{2}-\d{2} UTC'
        
        # Find all positions where timestamp lines start
        timestamp_positions = [m.start() for m in re.finditer(timestamp_pattern, content, re.MULTILINE)]
        
        if not timestamp_positions:
            print("Warning: Can't distinguish headers--bad formatting? Set JAERO to output format 3.", file=sys.stderr)
            return
        
        # Extract messages using these positions
        messages = []
        for i in range(len(timestamp_positions)):
            start_pos = timestamp_positions[i]
            end_pos = timestamp_positions[i+1] if i+1 < len(timestamp_positions) else len(content)
            message = content[start_pos:end_pos].strip()
            messages.append(message)
        
        # Dictionary to store messages by label
        messages_by_label = {}
        unlabeled_messages = []
        
        # Extract label from each message and group
        for message in messages:
            # Find the label (we're looking for something like "! H1 F" and want to extract "H1")
            match = re.search(r'! ([A-Za-z0-9]+)', message)
            if match:
                label = match.group(1)
                if label not in messages_by_label:
                    messages_by_label[label] = []
                messages_by_label[label].append(message)
            else:
                unlabeled_messages.append(message)
        
        if not messages_by_label:
            print("Warning: No messages with labels found in the log file.", file=sys.stderr)
            return
        
        # Write messages to separate files by label
        for label, msg_list in messages_by_label.items():
            output_file = os.path.join(output_dir, f"acars_{label}.txt")
            with open(output_file, 'w') as file:
                file.write('\n\n'.join(msg_list))
            print(f"Created {output_file} with {len(msg_list)} messages")
        
        # Write unlabeled messages if any
        if unlabeled_messages:
            output_file = os.path.join(output_dir, "acars_unlabeled.txt")
            with open(output_file, 'w') as file:
                file.write('\n\n'.join(unlabeled_messages))
            print(f"Created {output_file} with {len(unlabeled_messages)} messages")
    
    except Exception as e:
        print(f"Error processing file: {str(e)}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Split ACARS log by message label.')
    parser.add_argument('input_file', help='Path to the ACARS log file')
    parser.add_argument('-o', '--output-dir', default='acars_by_label',
                        help='Directory to store output files (default: acars_by_label)')
    
    args = parser.parse_args()
    parse_acars_log(args.input_file, args.output_dir)

if __name__ == "__main__":
    main()
