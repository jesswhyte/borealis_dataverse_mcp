#!/usr/bin/env python3
import asyncio
import os
import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Configuration
BOREALIS_BASE_URL = "https://borealisdata.ca/api"
API_KEY = os.environ.get("BOREALIS_API_KEY", "")

# Mapping of university names to dataverse identifiers
UNIVERSITY_DATAVERSE_MAP = {
    "algoma university": "algoma",
    "algoma": "algoma",
    "sunnybrook research institute": "sunnybrook",
    "sunnybrook": "sunnybrook",
    "university of victoria": "uvic",
    "uvic": "uvic",
    "acadia university": "acadia",
    "acadia": "acadia",
    "athabasca university": "athabascau",
    "athabasca": "athabascau",
    "bishops university": "bishops",
    "bishops": "bishops",
    "brandon university": "brandon",
    "brandon": "brandon",
    "brock university": "brock",
    "brock": "brock",
    "cape breton university": "capebreton",
    "cape breton": "capebreton",
    "carleton university": "carleton",
    "carleton": "carleton",
    "concordia": "concordia",
    "concordia university": "concordia",
    "dalhousie": "dal",
    "dalhousie university": "dal",
    "dal": "dal",
    "durham": "durham",
    "durham college": "durham",
    "enap": "enap",
    "ets": "ets",
    "cegep federation": "federationcegeps",
    "federation cegeps": "federationcegeps",
    "fanshawe": "fanshawe",
    "fanshawe college": "fanshawe",
    "georgian college": "georgian",
    "georgian": "georgian",
    "hec montreal": "hec",
    "hec": "hec",
    "inrs": "inrs",
    "lakehead": "lakehead",
    "lakehead university": "lakehead",
    "laurentian": "laurentian",
    "laurentian university": "laurentian",
    "macewan": "macewan",
    "macewan university": "macewan",
    "mcgill": "mcgill",
    "mcgill university": "mcgill",
    "mcmaster": "mcmaster",
    "mcmaster university": "mcmaster",
    "memorial": "memorial",
    "memorial university": "memorial",
    "mount allison": "mta",
    "mount allison university": "mta",
    "mta": "mta",
    "mount royal": "mru",
    "mount royal university": "mru",
    "mru": "mru",
    "mount saint vincent": "msvu",
    "mount saint vincent university": "msvu",
    "msvu": "msvu",
    "nipissing": "nipissing",
    "nipissing university": "nipissing",
    "ocad": "ocad",
    "ocad university": "ocad",
    "ontario tech": "ontariotechu",
    "ontario tech university": "ontariotechu",
    "ontariotechu": "ontariotechu",
    "polytechnique montreal": "polymtl",
    "polytechnique": "polymtl",
    "polymtl": "polymtl",
    "queens": "queens",
    "queens university": "queens",
    "queen's": "queens",
    "queen's university": "queens",
    "royal military college": "rmc",
    "rmc": "rmc",
    "royal roads": "rru",
    "royal roads university": "rru",
    "rru": "rru",
    "saint mary's": "smu",
    "saint mary's university": "smu",
    "smu": "smu",
    "saint francis xavier": "stfx",
    "saint francis xavier university": "stfx",
    "st francis xavier": "stfx",
    "stfx": "stfx",
    "thompson rivers": "tru",
    "thompson rivers university": "tru",
    "tru": "tru",
    "toronto metropolitan university": "tmu",
    "toronto metropolitan": "tmu",
    "tmu": "tmu",
    "ryerson": "tmu",
    "ryerson university": "tmu",
    "trent": "trent",
    "trent university": "trent",
    "trinity western": "twu",
    "trinity western university": "twu",
    "twu": "twu",
    "universite de montreal": "montreal",
    "université de montréal": "montreal",
    "university of montreal": "montreal",
    "montreal": "montreal",
    "université de saint-boniface": "USB",
    "universite de saint-boniface": "USB",
    "usb": "USB",
    "université de sherbrooke": "udes",
    "universite de sherbrooke": "udes",
    "university of sherbrooke": "udes",
    "sherbrooke": "udes",
    "udes": "udes",
    "uqac": "uqac",
    "uqam": "uqam",
    "université du québec en abitibi-témiscamingue": "uqat",
    "universite du quebec en abitibi-temiscamingue": "uqat",
    "uqat": "uqat",
    "université du québec à rimouski": "uqar",
    "universite du quebec a rimouski": "uqar",
    "uqar": "uqar",
    "université du québec à trois-rivières": "uqtr",
    "universite du quebec a trois-rivieres": "uqtr",
    "uqtr": "uqtr",
    "université du québec en outaouais": "uqo",
    "universite du quebec en outaouais": "uqo",
    "uqo": "uqo",
    "université laval": "laval",
    "universite laval": "laval",
    "laval": "laval",
    "université téluq": "teluq",
    "universite teluq": "teluq",
    "teluq": "teluq",
    "university of alberta": "ualberta",
    "ualberta": "ualberta",
    "alberta": "ualberta",
    "u of a": "ualberta",
    "university of british columbia": "ubc",
    "ubc": "ubc",
    "british columbia": "ubc",
    "university of calgary": "calgary",
    "calgary": "calgary",
    "u of c": "calgary",
    "university of guelph": "guelph",
    "guelph": "guelph",
    "university of lethbridge": "lethbridge",
    "lethbridge": "lethbridge",
    "university of manitoba": "manitoba",
    "manitoba": "manitoba",
    "u of m": "manitoba",
    "university of northern british columbia": "unbc",
    "unbc": "unbc",
    "northern british columbia": "unbc",
    "university of ottawa": "ottawa",
    "ottawa": "ottawa",
    "uottawa": "ottawa",
    "u of o": "ottawa",
    "university of regina": "regina",
    "regina": "regina",
    "university of toronto": "toronto",
    "toronto": "toronto",
    "u of t": "toronto",
    "uoft": "toronto",
    "university of waterloo": "waterloo",
    "waterloo": "waterloo",
    "uwaterloo": "waterloo",
    "university of windsor": "windsor",
    "windsor": "windsor",
    "university of winnipeg": "uwinnipeg",
    "winnipeg": "uwinnipeg",
    "uwinnipeg": "uwinnipeg",
    "vancouver island university": "viu",
    "vancouver island": "viu",
    "viu": "viu",
    "western university": "western",
    "western": "western",
    "uwo": "western",
    "wilfred laurier university": "laurier",
    "wilfrid laurier": "laurier",
    "laurier": "laurier",
    "wlu": "laurier",
    "york university": "york",
    "york": "york",
}

# Create MCP server
app = Server("borealis-dataverse")

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools for Borealis Dataverse."""
    return [
        Tool(
            name="search_datasets",
            description="Search for datasets in Borealis Dataverse by keywords, subjects, or other criteria. Returns a list of matching datasets with basic information. When presenting results to the user, ALWAYS include the full DOI URL (e.g., https://doi.org/10.34990/FK2/XXXXX) for each dataset in your summary.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query string (searches titles, descriptions, keywords, etc.)"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Number of results per page (default: 10, max: 100)",
                        "default": 10
                    },
                    "sort": {
                        "type": "string",
                        "description": "Sort order: 'name' (alphabetical), 'date' (newest first), or 'relevance' (default)",
                        "enum": ["name", "date", "relevance"],
                        "default": "relevance"
                    },
                    "type": {
                        "type": "string",
                        "description": "Filter by type: 'dataset', 'dataverse', or 'file'. Leave empty for all types.",
                        "enum": ["dataset", "dataverse", "file"]
                    },
                    "dataverse": {
                        "type": "string",
                        "description": "Optional: Limit search to a specific university/institution dataverse. Can be specified as either the university name (e.g., 'University of Toronto', 'UBC') or the dataverse identifier (e.g., 'toronto', 'ubc'). Supports all major Canadian universities and colleges including University of Toronto, University of Alberta, UBC, McGill, Concordia, Dalhousie, and many others."
                    }
                },
                "required": ["query"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""
    if name == "search_datasets":
        return await search_datasets(arguments)
    else:
        raise ValueError(f"Unknown tool: {name}")

async def search_datasets(arguments: dict) -> list[TextContent]:
    """Search for datasets in Borealis Dataverse."""
    query = arguments.get("query", "*")
    per_page = arguments.get("per_page", 10)
    sort_field = arguments.get("sort", "relevance")
    result_type = arguments.get("type")
    dataverse = arguments.get("dataverse")
    
    # Validate per_page
    if per_page > 100:
        per_page = 100
    
    # If dataverse is specified, try to map university name to identifier
    if dataverse:
        # Normalize the input (lowercase, strip whitespace)
        normalized = dataverse.lower().strip()
        # Look up in mapping, or use as-is if not found (might already be an identifier)
        dataverse = UNIVERSITY_DATAVERSE_MAP.get(normalized, dataverse)
    
    # Build query parameters
    params = {
        "q": query,
        "per_page": per_page,
    }
    
    # Add sort parameters if not relevance (default)
    if sort_field != "relevance":
        params["sort"] = sort_field
        params["order"] = "desc" if sort_field == "date" else "asc"
    
    # Add type filter if specified
    if result_type:
        params["type"] = result_type
    
    # Add dataverse/subtree filter if specified
    if dataverse:
        params["subtree"] = dataverse
    
    # Prepare headers - only add API key if it exists and looks valid
    headers = {}
    use_auth = False
    if API_KEY and len(API_KEY) > 10:  # Basic validation
        headers["X-Dataverse-key"] = API_KEY
        use_auth = True
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{BOREALIS_BASE_URL}/search",
                params=params,
                headers=headers
            )
            
            # If we get a 401 with auth, try again without auth for public search
            if response.status_code == 401 and use_auth:
                headers = {}  # Remove auth header
                response = await client.get(
                    f"{BOREALIS_BASE_URL}/search",
                    params=params,
                    headers=headers
                )
            
            response.raise_for_status()
            data = response.json()
        
        # Check if the response was successful
        if data.get("status") != "OK":
            return [TextContent(
                type="text",
                text=f"Error: API returned status '{data.get('status')}'"
            )]
        
        # Extract search results
        search_data = data.get("data", {})
        items = search_data.get("items", [])
        total_count = search_data.get("total_count", 0)
        
        if total_count == 0:
            return [TextContent(
                type="text",
                text=f"No results found for query: '{query}'"
            )]
        
        # Format results
        result_text = f"Found {total_count} results for '{query}'\n"
        result_text += f"Showing {len(items)} results:\n\n"
        
        for idx, item in enumerate(items, 1):
            item_type = item.get("type", "unknown")
            name = item.get("name", "Untitled")
            url = item.get("url", "")
            description = item.get("description", "No description available")
            
            # Truncate long descriptions
            if len(description) > 200:
                description = description[:200] + "..."
            
            result_text += f"{idx}. [{name}]({url})\n"
            result_text += f"   Type: {item_type}\n"
            
            # Add dataset-specific information
            if item_type == "dataset":
                global_id = item.get("global_id", "")
                authors = item.get("authors", [])
                published_at = item.get("published_at", "")
                
                if global_id:
                    result_text += f"   DOI: {global_id}\n"
                if authors:
                    result_text += f"   Authors: {', '.join(authors)}\n"
                if published_at:
                    result_text += f"   Published: {published_at}\n"
            
            result_text += f"   Description: {description}\n\n"
        
        if total_count > len(items):
            result_text += f"\n(Showing {len(items)} of {total_count} total results. "
            result_text += "Adjust 'per_page' parameter to see more results.)"
        
        return [TextContent(type="text", text=result_text)]
        
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error occurred: {e.response.status_code}\n"
        try:
            error_data = e.response.json()
            error_msg += f"API Response: {error_data}\n"
        except:
            error_msg += f"Response: {e.response.text}\n"
        error_msg += "\nNote: Searches of published datasets don't require authentication."
        return [TextContent(type="text", text=error_msg)]
    except httpx.RequestError as e:
        error_msg = f"Request error occurred: {str(e)}"
        return [TextContent(type="text", text=error_msg)]
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        return [TextContent(type="text", text=error_msg)]

async def main():
    """Run the server using stdio transport."""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())