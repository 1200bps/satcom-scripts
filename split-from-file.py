import os
import re
import argparse
import sys

def extract_tail_number(message):
    """Extract the tail number from an ACARS message."""
    match = re.search(r'AES:[A-F0-9]+\s+GES:[A-Z0-9]+\s+\d+\s+(\.?[A-Za-z0-9-]+)', message)
    if match:
        return match.group(1).strip('.')
    return None

def determine_message_type(message):
    """Determine if a message is CPDLC, ADS-C, MIAM or other."""
    if "FANS-1/A CPDLC" in message:
        return "CPDLC"
    elif "ADS-C" in message:
        return "ADS-C"
    elif "MIAM" in message:
        return "MIAM"
    else:
        return "OTHER"

def contains_keyword(message, keyword):
    """Check if a message contains a specific keyword (case-insensitive)."""
    return keyword.lower() in message.lower()

def extract_message_label(message):
    """Extract the message label from an ACARS message."""
    match = re.search(r'!\s+([A-Za-z0-9]{2})\s+[A-Za-z0-9]', message)
    if match:
        return match.group(1)
    return None

def parse_acars_log(log_file_path, output_dir, split_by='label', keyword=None):
    """
    Parse ACARS log file and split messages based on specified criteria.
    
    Args:
        log_file_path (str): Path to the ACARS log file
        output_dir (str): Directory to store the output files
        split_by (str): Criteria to split by ('label', 'tail', 'type', 'keyword')
        keyword (str): Keyword to filter messages (used when split_by='keyword')
    """
    # Validate input file
    if not os.path.isfile(log_file_path):
        print(f"Error: Input file '{log_file_path}' not found.", file=sys.stderr)
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
        
        # Dictionary to store grouped messages
        grouped_messages = {}
        unclassified_messages = []
        
        # Group messages based on chosen criteria
        for message in messages:
            key = None
            
            if split_by == 'label':
                key = extract_message_label(message)
            elif split_by == 'tail':
                key = extract_tail_number(message)
            elif split_by == 'type':
                key = determine_message_type(message)
            elif split_by == 'keyword':
                if keyword and contains_keyword(message, keyword):
                    # For keyword filtering, use the keyword as the key
                    key = 'containing_' + keyword
            
            if key:
                if key not in grouped_messages:
                    grouped_messages[key] = []
                grouped_messages[key].append(message)
            else:
                unclassified_messages.append(message)
        
        if not grouped_messages:
            print(f"Warning: No messages could be classified by {split_by}.", file=sys.stderr)
            return
        
        # Write messages to separate files
        for key, msg_list in grouped_messages.items():
            if split_by == 'label':
                output_file = os.path.join(output_dir, f"acars_label_{key}.txt")
            elif split_by == 'tail':
                output_file = os.path.join(output_dir, f"acars_tail_{key}.txt")
            elif split_by == 'type':
                output_file = os.path.join(output_dir, f"acars_type_{key}.txt")
            elif split_by == 'keyword':
                output_file = os.path.join(output_dir, f"acars_{key}.txt")
            
            with open(output_file, 'w') as file:
                file.write('\n\n'.join(msg_list))
            print(f"Created {output_file} with {len(msg_list)} messages")
        
        # Write unclassified messages if any
        if unclassified_messages:
            if split_by == 'keyword':
                output_file = os.path.join(output_dir, f"acars_not_containing_{keyword}.txt")
            else:
                output_file = os.path.join(output_dir, f"acars_unclassified.txt")
            with open(output_file, 'w') as file:
                file.write('\n\n'.join(unclassified_messages))
            print(f"Created {output_file} with {len(unclassified_messages)} messages")
    
    except Exception as e:
        print(f"Error processing file: {str(e)}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Split ACARS log by various criteria.')
    parser.add_argument('input_file', help='Path to the ACARS log file')
    parser.add_argument('-o', '--output-dir', default='acars_split',
                        help='Directory to store output files (default: acars_split)')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-l', '--by-label', action='store_true',
                      help='Split messages by label (default)')
    group.add_argument('-t', '--by-tail', action='store_true',
                      help='Split messages by aircraft tail number')
    group.add_argument('-m', '--by-type', action='store_true',
                      help='Split messages by type (CPDLC, ADS-C, MIAM, OTHER)')
    group.add_argument('-k', '--keyword', metavar='KEYWORD',
                      help='Extract messages containing specific keyword')
    
    args = parser.parse_args()
    
    # Determine split criteria
    if args.by_tail:
        split_by = 'tail'
    elif args.by_type:
        split_by = 'type'
    elif args.keyword:
        split_by = 'keyword'
    else:
        # Default to label
        split_by = 'label'
    
    parse_acars_log(args.input_file, args.output_dir, split_by, args.keyword)

if __name__ == "__main__":
    main()
