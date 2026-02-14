# Borealis Dataverse MCP Server

This is a personal experimentation project and not affiliated with Borealis or Dataverse. It's also a work in progress. It is a custom MCP (Model Context Protocol) server that runs locally and enables a tool like Claude Desktop to search the [Borealis Dataverse](https://borealisdata.ca) - Canada's research data repository - directly from conversations.

**Note**: This MCP server was developed with assistance from Claude for Python coding. 


## What It Does

This connector allows:
- Searching through thousands of research datasets from Canadian universities
- Filter results by specific institutions (70+ Canadian universities supported)
- Return formatted results with titles, descriptions, DOI links, and authors
- Access both published and unpublished datasets (unpublished with API key and access permissions)

### Example Queries

- "Search Borealis for datasets about pollination"
- "Use my Borealis search tool to find datasets from the University of Toronto about bees"
- "Show me datasets from the last 5 years from UBC about forestry"

## Features

- **Search**: Query by keywords, subjects
- **University Filtering**: Automatic translation of university names to dataverse identifiers (e.g., "University of Toronto" → `toronto`)
- **Flexible Parameters**: Customize number of results, sorting, and type filtering
- **Authentication Support**: Optional API key for accessing unpublished datasets
- **Automatic Fallback**: Falls back to public search if authentication fails
- **Results**: Returns DOI links, authors, publication dates, and description summaries

## Prerequisites

- Claude Desktop or a local llm installed (Claude Desktop is used here in the instructions as an example)
- Python 3.7+
- A Borealis account and API key (optional - public searches work without authentication)

## Installation

### 1. Install Required Dependencies

```bash
pip install mcp httpx
```

### 2. Clone This Repository

```bash
git clone https://github.com/jesswhyte/borealis_dataverse_mcp.git
cd borealis_dataverse_mcp
```

### 3. Make the Server Executable

```bash
chmod +x borealis_server.py
```

### 4. Get Your Borealis API Key (Optional)

1. Go to https://borealisdata.ca
2. Log in or create an account
3. Navigate to your account settings
4. Generate an API token
5. Copy the token (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

### 5. Configure Claude Desktop

Edit your Claude Desktop configuration file, e.g. if you're on a Mac:

```bash
nano ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

Add the following configuration (note you will need to change the path to borealis_server.py and edit your API key):

```json
{
  "mcpServers": {
    "borealis-dataverse": {
      "command": "python3",
      "args": [
        "/absolute/path/to/borealis_server.py"
      ],
      "env": {
        "BOREALIS_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

**Important**: 
- Replace `/absolute/path/to/borealis_server.py` with the full path to your cloned repository
- Replace `your-api-key-here` with your actual Borealis API key
- If you don't have an API key, omit the `env` section entirely - public searches will still work

### 6. Restart Claude Desktop

- Quit Claude Desktop completely (⌘+Q)
- Reopen Claude Desktop

### 7. Verify Installation

Check the logs to confirm the server started successfully:

```bash
cat ~/Library/Logs/Claude/mcp*.log | grep borealis
```

You should see messages indicating the server started and connected.

### 8. Test It

Open a **new conversation** in Claude Desktop and try:

```
Search Borealis for datasets about pelagic species from the University of Alberta
```

## Usage

### Basic Search

```
Search Borealis for datasets about [your topic]
```

### Search by University

```
Find datasets from [University Name] about [topic]
```

Supported university formats:
- Full names: "University of Toronto", "McGill University"
- Some short names: "UBC", "U of T"
- Common abbreviations: "UAlberta", "UWaterloo"

### Advanced Options

The tool supports:
- **Number of results**: Request more or fewer results (max 100) in prompt
- **Sorting**: Sort by relevance (default), date, or name
- **Type filtering**: Filter by dataset, dataverse, or file

## Supported Universities

The server includes mappings for 70+ Canadian institutions including:

- Larger universities (U of T, UBC, McGill, Alberta, etc.)
- Regional universities (Brandon, Cape Breton, Lakehead, etc.)
- Quebec universities (Université de Montréal, Laval, UQAM, etc.)
- Colleges and institutes (Georgian, Fanshawe, OCAD, etc.)

See `borealis_server.py` for the complete list.

## Architecture

### Components

- **Language**: Python 3
- **MCP SDK**: Official Python MCP library from Anthropic
- **HTTP Library**: httpx for async API calls
- **Configuration**: Environment variables and JSON config file

### How It Works

1. receives a search request from the user
2. The MCP server translates university names to dataverse identifiers
3. The server queries the Borealis API with appropriate filters
4. Results are formatted and returned
5. Presents the results with DOI links and metadata

## Troubleshooting

### Server Not Connecting

**Check the logs:**
```bash
cat ~/Library/Logs/Claude/mcp*.log | tail -50
```

**Common issues:**
- Verify the file path in your config is correct and absolute
- Ensure the Python file is executable (`chmod +x borealis_server.py`)
- Check that Python 3 is available: `which python3`

### API Authentication Errors

The server automatically falls back to public search if authentication fails. To verify your API key:

```bash
curl -H "X-Dataverse-key: YOUR_KEY" "https://borealisdata.ca/api/search?q=test"
```

### Tool Not Appearing

- Ensure you started a **new conversation** after restarting Claude Desktop
- MCP tools only load into conversations created after the server connects
- Check that the server shows as connected in Claude Desktop

## Development

### Extending the Server

To add additional tools (e.g., `get_dataset_metadata`, `list_collections`):

1. Add the tool definition to `list_tools()`
2. Implement the handler function
3. Add the handler to `call_tool()`

See the Borealis API documentation for available endpoints: https://borealisdata.ca/guides/en/latest/api/

### Testing

Test the server directly:

```bash
cd /path/to/borealis-mcp-server
python3 borealis_server.py
```

The server should start and wait for input without errors.

## Technical Notes

- The server uses async/await for non-blocking API calls
- Authentication is optional; public searches work without an API key
- University name matching is case-insensitive
- The `subtree` parameter filters results to specific dataverses
- Results are limited to 100 per request (Borealis API limit)

## Contributing

Contributions are welcome! Areas for improvement:

- Additional search tools (metadata retrieval, file access)
- Support for more advanced Borealis API features
- Better error handling and user feedback
- Extended university mappings

## License

GPLv3 

## Acknowledgments

- Built using the [Model Context Protocol](https://modelcontextprotocol.io/)
- Developed with a lot of assistance from Claude 
- Powered by the amazing API at [Borealis Dataverse](https://borealisdata.ca)

## Support

For issues related to:
- **This MCP server**: Open an issue in this repository
- **Borealis API**: Visit https://borealisdata.ca/guides/
- **Claude Desktop**: Contact Anthropic support
- **MCP Protocol**: See https://modelcontextprotocol.io/

---


