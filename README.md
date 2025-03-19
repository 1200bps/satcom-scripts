# satcom-scripts
Tools to process logged or UDP-streamed Inmarsat ACARS messages from Jaero.

### split-from-file
Ingest an on-disk log of received messages and split messages out to multiple files based on one of several attributes:
 * `-l / --by-label`: Label field in ACARS header (e.g. `AA`, `A6`, `H1`)
 * `-t / --by-tail`: Aircraft tail number
 * `-m / --by-type`: Message type (CPDLC, ADS-C, MIAM, or "other")
 * `-k / --keyword KEYWORD`: Arbitrary keyword

Usage:
`python3 split-from-file.py acars-log.txt -m -o messages_by_type/`

### split-from-streams
Bind to an IP and accept messages by UDP on one or several ports, splitting to files by `label`, `tail`, `type`, `keyword`.

Usage:
`python3 split-from-streams.py config.json`
