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
    "victoria": "uvic",
    "acadia university": "acadia",
    "acadia": "acadia",
    "athabasca university": "athabascau",
    "athabasca": "athabascau",
    "bishops university": "bishops",
    "bishops": "bishops",
    "bishop's university": "bishops",
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
    "école nationale d'administration publique": "enap",
    "ecole nationale d'administration publique": "enap",
    "ets": "ets",
    "école de technologie supérieure": "ets",
    "ecole de technologie superieure": "ets",
    "cegep federation": "federationcegeps",
    "federation cegeps": "federationcegeps",
    "fanshawe": "fanshawe",
    "fanshawe college": "fanshawe",
    "georgian college": "georgian",
    "georgian": "georgian",
    "hec montreal": "hec",
    "hec montréal": "hec",
    "hec": "hec",
    "inrs": "inrs",
    "institut national de la recherche scientifique": "inrs",
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
    "ontario college of art and design": "ocad",
    "ontario tech": "ontariotechu",
    "ontario tech university": "ontariotechu",
    "ontariotechu": "ontariotechu",
    "polytechnique montreal": "polymtl",
    "polytechnique montréal": "polymtl",
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
    "st mary's": "smu",
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
    "udem": "montreal",
    "université de saint-boniface": "USB",
    "universite de saint-boniface": "USB",
    "usb": "USB",
    "université de sherbrooke": "udes",
    "universite de sherbrooke": "udes",
    "university of sherbrooke": "udes",
    "sherbrooke": "udes",
    "udes": "udes",
    "uqac": "uqac",
    "université du québec à chicoutimi": "uqac",
    "universite du quebec a chicoutimi": "uqac",
    "uqam": "uqam",
    "université du québec à montréal": "uqam",
    "universite du quebec a montreal": "uqam",
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
    "téluq": "teluq",
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
    "ucalgary": "calgary",
    "university of guelph": "guelph",
    "guelph": "guelph",
    "university of lethbridge": "lethbridge",
    "lethbridge": "lethbridge",
    "ulethbridge": "lethbridge",
    "university of manitoba": "manitoba",
    "manitoba": "manitoba",
    "u of m": "manitoba",
    "umanitoba": "manitoba",
    "university of northern british columbia": "unbc",
    "unbc": "unbc",
    "northern british columbia": "unbc",
    "university of ottawa": "ottawa",
    "ottawa": "ottawa",
    "uottawa": "ottawa",
    "u of o": "ottawa",
    "university of regina": "regina",
    "regina": "regina",
    "uregina": "regina",
    "university of toronto": "toronto",
    "toronto": "toronto",
    "u of t": "toronto",
    "uoft": "toronto",
    "ut": "toronto",
    "university of waterloo": "waterloo",
    "waterloo": "waterloo",
    "uwaterloo": "waterloo",
    "university of windsor": "windsor",
    "windsor": "windsor",
    "uwindsor": "windsor",
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
    "wilfrid laurier university": "laurier",
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
            description="Search for datasets in Borealis Dataverse by keywords, subjects, or other criteria. Returns a list of matching datasets with basic information. When presenting results to the user, ALWAYS include the full DOI URL (e.g., https://doi.org/10.34990/FK2/XXXXX) and the authors for each dataset in your summary. Do not omit author information.",
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
                    },
                    "country": {
                        "type": "string",
                        "description": "Optional: Filter by the geographic coverage/subject area of datasets (e.g., datasets ABOUT 'Canada', 'United States'). This indicates what region the data describes, not where researchers are located."
                    },
                    "province": {
                        "type": "string",
                        "description": "Optional: Filter by the geographic coverage/subject area of datasets (e.g., datasets ABOUT 'Ontario', 'Nova Scotia', 'British Columbia', 'Quebec'). This indicates what province/state the data describes, not where researchers are located."
                    },
                    "city": {
                        "type": "string",
                        "description": "Optional: Filter by the geographic coverage/subject area of datasets (e.g., datasets ABOUT 'Toronto', 'Halifax', 'Vancouver'). This indicates what city the data describes, not where researchers are located."
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_dataset_metadata",
            description="Retrieve detailed metadata for a specific dataset from Borealis Dataverse. Use this when the user asks for more information about a specific dataset found in search results. Returns comprehensive metadata including full description, authors, keywords, subjects, file information, and more.",
            inputSchema={
                "type": "object",
                "properties": {
                    "identifier": {
                        "type": "string",
                        "description": "Dataset identifier - can be either a DOI (e.g., 'doi:10.34990/FK2/ABC123' or 'https://doi.org/10.34990/FK2/ABC123') or a numeric database ID. DOIs are preferred."
                    }
                },
                "required": ["identifier"]
            }
        ),
        Tool(
            name="list_dataset_files",
            description="List all files in a specific dataset from Borealis Dataverse. Use this when the user asks what files are in a dataset or wants to see the file list. Returns file names, sizes, formats, descriptions, and access restrictions. By default shows first 20 files; user can request more or filter by file type.",
            inputSchema={
                "type": "object",
                "properties": {
                    "identifier": {
                        "type": "string",
                        "description": "Dataset identifier - can be either a DOI (e.g., 'doi:10.34990/FK2/ABC123' or 'https://doi.org/10.34990/FK2/ABC123') or a numeric database ID. DOIs are preferred."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of files to return (default: 20). Use higher values if user wants to see more files.",
                        "default": 20
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Number of files to skip (for pagination). Use to show additional files beyond the initial results.",
                        "default": 0
                    },
                    "file_type": {
                        "type": "string",
                        "description": "Optional: Filter by file type or search in filenames (e.g., 'csv', 'readme', 'pdf', 'spss', 'data'). Searches in both filename and friendly type."
                    }
                },
                "required": ["identifier"]
            }
        ),
        Tool(
            name="get_dataset_file",
            description="Download and retrieve the content of a specific file from a Borealis dataset. Use this when the user wants to examine, analyze, or explore a specific file. IMPORTANT: Only supports text-based files under 5MB. Binary files (PDF, ZIP, Excel) and large data files are not suitable for chat display. The file content will be truncated to 100 lines if longer.",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_id": {
                        "type": "string",
                        "description": "The numeric file ID from the file list (e.g., '276461'). Get this from list_dataset_files."
                    },
                    "filename": {
                        "type": "string",
                        "description": "Optional: The filename for context (helps with user-friendly error messages)."
                    }
                },
                "required": ["file_id"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""
    if name == "search_datasets":
        return await search_datasets(arguments)
    elif name == "get_dataset_metadata":
        return await get_dataset_metadata(arguments)
    elif name == "list_dataset_files":
        return await list_dataset_files(arguments)
    elif name == "get_dataset_file":
        return await get_dataset_file(arguments)
    else:
        raise ValueError(f"Unknown tool: {name}")

def format_authors(authors: list) -> str:
    """Format author list - show all if 3 or fewer, otherwise show first + et al."""
    if not authors:
        return "No authors listed"
    
    if len(authors) <= 3:
        return ", ".join(authors)
    else:
        return f"{authors[0]} et al."

def format_date(date_string: str) -> str:
    """Format date string to show just the year."""
    if not date_string:
        return "No date available"
    
    # Extract just the year (first 4 characters)
    return date_string[:4]

async def search_datasets(arguments: dict) -> list[TextContent]:
    """Search for datasets in Borealis Dataverse."""
    query = arguments.get("query", "*")
    per_page = arguments.get("per_page", 10)
    sort_field = arguments.get("sort", "relevance")
    result_type = arguments.get("type")
    dataverse = arguments.get("dataverse")
    country = arguments.get("country")
    province = arguments.get("province")
    city = arguments.get("city")
    
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
    
    # Add geographic filters using fq (filter query) parameter
    # Multiple fq parameters can be combined
    if country:
        params["fq"] = f"country:{country}"
    
    if province:
        # If we already have an fq param, we need to handle multiple filters
        # The API accepts multiple fq parameters as separate query params
        fq_value = f"state:{province}"
        if "fq" in params:
            # httpx will handle multiple values for the same parameter
            if isinstance(params["fq"], list):
                params["fq"].append(fq_value)
            else:
                params["fq"] = [params["fq"], fq_value]
        else:
            params["fq"] = fq_value
    
    if city:
        fq_value = f"city:{city}"
        if "fq" in params:
            if isinstance(params["fq"], list):
                params["fq"].append(fq_value)
            else:
                params["fq"] = [params["fq"], fq_value]
        else:
            params["fq"] = fq_value
    
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
        
        # Format results with consistent structure
        result_text = f"Found {total_count} results for '{query}'\n"
        result_text += f"Showing {len(items)} results:\n\n"
        
        for idx, item in enumerate(items, 1):
            item_type = item.get("type", "unknown")
            name = item.get("name", "Untitled")
            url = item.get("url", "")
            description = item.get("description", "No description available")
            
            # Truncate long descriptions to ~150 characters
            if len(description) > 150:
                description = description[:150] + "..."
            
            # Start with title and type
            result_text += f"{idx}. **{name}**\n"
            result_text += f"   Type: {item_type}\n"
            
            # For datasets, always show: DOI, Authors, Date, Description
            if item_type == "dataset":
                # DOI (required field)
                global_id = item.get("global_id", "")
                if global_id:
                    # Convert DOI to full URL if it's not already
                    doi_url = global_id if global_id.startswith("http") else f"https://doi.org/{global_id.replace('doi:', '')}"
                    result_text += f"   DOI: {doi_url}\n"
                else:
                    result_text += f"   DOI: {url}\n"  # Fallback to dataset URL
                
                # Authors (required field)
                authors = item.get("authors", [])
                result_text += f"   Authors: {format_authors(authors)}\n"
                
                # Date (required field)
                published_at = item.get("published_at", "")
                result_text += f"   Date: {format_date(published_at)}\n"
                
                # Description (required field)
                result_text += f"   Description: {description}\n"
            
            # For dataverses and files, show simpler info
            else:
                if url:
                    result_text += f"   URL: {url}\n"
                result_text += f"   Description: {description}\n"
            
            result_text += "\n"
        
        if total_count > len(items):
            result_text += f"(Showing {len(items)} of {total_count} total results. "
            result_text += "Adjust 'per_page' parameter to see more results.)\n"
        
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

async def get_dataset_metadata(arguments: dict) -> list[TextContent]:
    """Retrieve detailed metadata for a specific dataset."""
    identifier = arguments.get("identifier", "")
    
    if not identifier:
        return [TextContent(
            type="text",
            text="Error: No dataset identifier provided."
        )]
    
    # Clean up the identifier
    # If it's a full DOI URL, extract just the DOI part
    if identifier.startswith("http"):
        # Extract DOI from URL like https://doi.org/10.34990/FK2/ABC123
        identifier = identifier.split("doi.org/")[-1]
        if not identifier.startswith("doi:"):
            identifier = f"doi:{identifier}"
    elif not identifier.startswith("doi:") and not identifier.isdigit():
        # If it looks like a DOI but doesn't have the prefix, add it
        identifier = f"doi:{identifier}"
    
    # Build the API URL
    # Use persistentId parameter for DOIs, or direct ID for numeric IDs
    if identifier.startswith("doi:"):
        api_url = f"{BOREALIS_BASE_URL}/datasets/:persistentId/metadata"
        params = {"persistentId": identifier}
    else:
        # Numeric dataset ID
        api_url = f"{BOREALIS_BASE_URL}/datasets/{identifier}/metadata"
        params = {}
    
    # Prepare headers
    headers = {
        "Accept": "application/ld+json"
    }
    use_auth = False
    if API_KEY and len(API_KEY) > 10:
        headers["X-Dataverse-key"] = API_KEY
        use_auth = True
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                api_url,
                params=params,
                headers=headers
            )
            
            # If we get a 401 with auth, try again without auth for public datasets
            if response.status_code == 401 and use_auth:
                headers = {
                    "Accept": "application/ld+json"
                }
                response = await client.get(
                    api_url,
                    params=params,
                    headers=headers
                )
            
            response.raise_for_status()
            response_data = response.json()
        
        # Check if response was successful
        if response_data.get("status") != "OK":
            return [TextContent(
                type="text",
                text=f"Error: API returned status '{response_data.get('status')}'"
            )]
        
        # Extract the actual metadata from the 'data' field
        metadata = response_data.get("data", {})
        
        if not metadata:
            return [TextContent(
                type="text",
                text="Error: No metadata found in API response."
            )]
        
        # Parse and format the metadata
        result_text = "# Dataset Metadata\n\n"
        
        # Title
        title = metadata.get("title", metadata.get("schema:name", "No title available"))
        result_text += f"**Title:** {title}\n\n"
        
        # DOI/Identifier
        doi = metadata.get("@id", "")
        if doi:
            result_text += f"**DOI:** {doi}\n\n"
        
        # Description (from citation:dsDescription)
        description_obj = metadata.get("citation:dsDescription", {})
        if isinstance(description_obj, dict):
            description = description_obj.get("citation:dsDescriptionValue", "No description available")
        else:
            description = metadata.get("schema:description", "No description available")
        
        # Strip HTML tags for cleaner display if present
        import re
        description_text = re.sub(r'<[^>]+>', '', description)
        # Limit description length for display
        if len(description_text) > 500:
            description_text = description_text[:500] + "..."
        result_text += f"**Description:** {description_text}\n\n"
        
        # Authors/Creators
        authors = metadata.get("author", [])
        if authors:
            author_list = []
            for author in authors:
                if isinstance(author, dict):
                    name = author.get("citation:authorName", "")
                    affiliation = author.get("citation:authorAffiliation", "")
                    if name:
                        if affiliation:
                            author_list.append(f"{name} ({affiliation})")
                        else:
                            author_list.append(name)
            if author_list:
                result_text += f"**Authors:**\n"
                for author in author_list:
                    result_text += f"  - {author}\n"
                result_text += "\n"
        
        # Publication Date
        date_published = metadata.get("schema:datePublished", metadata.get("dateOfDeposit", ""))
        if date_published:
            result_text += f"**Publication Date:** {format_date(date_published)}\n\n"
        
        # Keywords
        keywords = metadata.get("citation:keyword", [])
        if keywords:
            keyword_list = []
            for kw in keywords:
                if isinstance(kw, dict):
                    keyword_list.append(kw.get("citation:keywordValue", ""))
                else:
                    keyword_list.append(str(kw))
            if keyword_list:
                result_text += f"**Keywords:** {', '.join(keyword_list)}\n\n"
        
        # Subject
        subject = metadata.get("subject", "")
        if subject:
            result_text += f"**Subject:** {subject}\n\n"
        
        # License
        license_info = metadata.get("schema:license", "")
        if license_info:
            result_text += f"**License:** {license_info}\n\n"
        
        # Alternative URL (often HuggingFace, GitHub, etc.)
        alt_url = metadata.get("alternativeURL", "")
        if alt_url:
            result_text += f"**Alternative URL:** {alt_url}\n\n"
        
        # Dataverse/Collection
        part_of = metadata.get("schema:isPartOf", {})
        if isinstance(part_of, dict):
            collection_name = part_of.get("schema:name", "")
            if collection_name:
                result_text += f"**Collection:** {collection_name}\n\n"
        
        # Contact
        contacts = metadata.get("citation:datasetContact", [])
        if contacts:
            contact_list = []
            for contact in contacts:
                if isinstance(contact, dict):
                    name = contact.get("citation:datasetContactName", "")
                    affiliation = contact.get("citation:datasetContactAffiliation", "")
                    if name:
                        if affiliation:
                            contact_list.append(f"{name} ({affiliation})")
                        else:
                            contact_list.append(name)
            if contact_list:
                result_text += f"**Contact:** {', '.join(contact_list)}\n\n"
        
        # Version
        version = metadata.get("schema:version", "")
        if version:
            result_text += f"**Version:** {version}\n\n"
        
        # Status
        status = metadata.get("schema:creativeWorkStatus", "")
        if status:
            result_text += f"**Status:** {status}\n\n"
        
        return [TextContent(type="text", text=result_text)]
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            error_msg = f"Dataset not found: {identifier}\n"
            error_msg += "Please check the DOI or dataset ID and try again."
        else:
            error_msg = f"HTTP error occurred: {e.response.status_code}\n"
            try:
                error_data = e.response.json()
                error_msg += f"API Response: {error_data}\n"
            except:
                error_msg += f"Response: {e.response.text}\n"
        return [TextContent(type="text", text=error_msg)]
    except httpx.RequestError as e:
        error_msg = f"Request error occurred: {str(e)}"
        return [TextContent(type="text", text=error_msg)]
    except Exception as e:
        error_msg = f"Unexpected error retrieving metadata: {str(e)}"
        return [TextContent(type="text", text=error_msg)]

async def list_dataset_files(arguments: dict) -> list[TextContent]:
    """List all files in a specific dataset."""
    identifier = arguments.get("identifier", "")
    limit = arguments.get("limit", 20)
    offset = arguments.get("offset", 0)
    file_type_filter = arguments.get("file_type", "")
    
    if not identifier:
        return [TextContent(
            type="text",
            text="Error: No dataset identifier provided."
        )]
    
    # Clean up the identifier (same logic as get_dataset_metadata)
    if identifier.startswith("http"):
        identifier = identifier.split("doi.org/")[-1]
        if not identifier.startswith("doi:"):
            identifier = f"doi:{identifier}"
    elif not identifier.startswith("doi:") and not identifier.isdigit():
        identifier = f"doi:{identifier}"
    
    # Build the API URL for files endpoint
    api_url = f"{BOREALIS_BASE_URL}/datasets/:persistentId/versions/:latest/files"
    params = {
        "persistentId": identifier,
        "limit": limit,
        "offset": offset
    }
    
    # Prepare headers
    headers = {}
    use_auth = False
    if API_KEY and len(API_KEY) > 10:
        headers["X-Dataverse-key"] = API_KEY
        use_auth = True
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                api_url,
                params=params,
                headers=headers
            )
            
            # If we get a 401 with auth, try again without auth for public datasets
            if response.status_code == 401 and use_auth:
                headers = {}
                response = await client.get(
                    api_url,
                    params=params,
                    headers=headers
                )
            
            response.raise_for_status()
            response_data = response.json()
        
        # Check if response was successful
        if response_data.get("status") != "OK":
            return [TextContent(
                type="text",
                text=f"Error: API returned status '{response_data.get('status')}'"
            )]
        
        # Extract files from the response
        files = response_data.get("data", [])
        total_count = response_data.get("totalCount", len(files))
        
        # Apply client-side filtering if file_type is specified
        if file_type_filter:
            filter_lower = file_type_filter.lower()
            filtered_files = []
            for file_info in files:
                data_file = file_info.get("dataFile", {})
                filename = data_file.get("filename", "").lower()
                friendly_type = data_file.get("friendlyType", "").lower()
                
                # Search in filename or friendly type
                if filter_lower in filename or filter_lower in friendly_type:
                    filtered_files.append(file_info)
            
            files = filtered_files
            # Note: total_count still reflects total files in dataset, not filtered count
        
        if not files:
            filter_msg = f" matching '{file_type_filter}'" if file_type_filter else ""
            return [TextContent(
                type="text",
                text=f"No files found{filter_msg} in this dataset."
            )]
        
        # Format the file list
        result_text = f"# Dataset Files\n\n"
        
        if file_type_filter:
            result_text += f"**Showing:** Files matching '{file_type_filter}'\n"
            result_text += f"**Results:** {len(files)} file(s) found\n"
        else:
            result_text += f"**Total files in dataset:** {total_count}\n"
            if total_count > limit:
                result_text += f"**Showing:** {offset + 1}-{min(offset + len(files), total_count)} of {total_count}\n"
        
        result_text += "\n"
        
        for idx, file_info in enumerate(files, offset + 1):
            # Extract file information
            label = file_info.get("label", "Unnamed file")
            description = file_info.get("description", "")
            restricted = file_info.get("restricted", False)
            
            # Get detailed file info from dataFile object
            data_file = file_info.get("dataFile", {})
            file_id = data_file.get("id", "")
            filename = data_file.get("filename", label)
            friendly_type = data_file.get("friendlyType", "Unknown")
            filesize = data_file.get("filesize", 0)
            md5 = data_file.get("md5", "")
            
            # Format file size for readability
            if filesize < 1024:
                size_str = f"{filesize} bytes"
            elif filesize < 1024 * 1024:
                size_str = f"{filesize / 1024:.1f} KB"
            elif filesize < 1024 * 1024 * 1024:
                size_str = f"{filesize / (1024 * 1024):.1f} MB"
            else:
                size_str = f"{filesize / (1024 * 1024 * 1024):.2f} GB"
            
            # Build file entry
            result_text += f"## {idx}. {filename}\n"
            if description:
                result_text += f"**Description:** {description}\n"
            result_text += f"**Type:** {friendly_type}\n"
            result_text += f"**Size:** {size_str}\n"
            result_text += f"**File ID:** {file_id}\n"
            
            if restricted:
                result_text += f"**Access:** Restricted (authentication required)\n"
            else:
                result_text += f"**Access:** Public\n"
            
            if md5:
                result_text += f"**MD5 Checksum:** {md5}\n"
            
            result_text += "\n"
        
        # Add pagination hint if there are more files
        if not file_type_filter and total_count > (offset + len(files)):
            remaining = total_count - (offset + len(files))
            result_text += f"\n*There are {remaining} more file(s) in this dataset. "
            result_text += f"To see more, ask to show the next files or specify a larger limit.*\n"
        
        return [TextContent(type="text", text=result_text)]
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            error_msg = f"Dataset not found: {identifier}\n"
            error_msg += "Please check the DOI or dataset ID and try again."
        else:
            error_msg = f"HTTP error occurred: {e.response.status_code}\n"
            try:
                error_data = e.response.json()
                error_msg += f"API Response: {error_data}\n"
            except:
                error_msg += f"Response: {e.response.text}\n"
        return [TextContent(type="text", text=error_msg)]
    except httpx.RequestError as e:
        error_msg = f"Request error occurred: {str(e)}"
        return [TextContent(type="text", text=error_msg)]
    except Exception as e:
        error_msg = f"Unexpected error listing files: {str(e)}"
        return [TextContent(type="text", text=error_msg)]

async def get_dataset_file(arguments: dict) -> list[TextContent]:
    """Download and retrieve content of a specific file from a dataset."""
    file_id = arguments.get("file_id", "")
    filename = arguments.get("filename", "file")
    
    if not file_id:
        return [TextContent(
            type="text",
            text="Error: No file ID provided. Use list_dataset_files to get file IDs."
        )]
    
    # Define supported text file extensions
    TEXT_EXTENSIONS = [
        '.txt', '.csv', '.tsv', '.dat', '.sps', '.r', '.py', '.json', 
        '.md', '.readme', '.do', '.sas', '.sql', '.xml', '.log', '.sh',
        '.yaml', '.yml', '.ini', '.cfg', '.conf'
    ]
    
    # Define binary extensions to explicitly reject
    BINARY_EXTENSIONS = [
        '.pdf', '.zip', '.xlsx', '.xls', '.sav', '.dta', '.rdata', 
        '.rds', '.docx', '.doc', '.pptx', '.ppt', '.jpg', '.jpeg', 
        '.png', '.gif', '.exe', '.dll', '.bin'
    ]
    
    # Check if filename suggests binary format
    filename_lower = filename.lower()
    is_likely_text = any(filename_lower.endswith(ext) for ext in TEXT_EXTENSIONS)
    is_binary = any(filename_lower.endswith(ext) for ext in BINARY_EXTENSIONS)
    
    if is_binary:
        return [TextContent(
            type="text",
            text=f"⚠️ Cannot retrieve '{filename}' - Binary file format not supported.\n\n"
                 f"This tool only supports text-based files (CSV, TXT, DAT, R, Python, etc.) "
                 f"that can be displayed in chat. Binary files like PDF, Excel, ZIP, and SPSS "
                 f"data files (.sav) must be downloaded separately through the Borealis website."
        )]
    
    # Build the API URL for file access
    api_url = f"{BOREALIS_BASE_URL}/access/datafile/{file_id}"
    
    # Prepare headers
    headers = {}
    use_auth = False
    if API_KEY and len(API_KEY) > 10:
        headers["X-Dataverse-key"] = API_KEY
        use_auth = True
    
    try:
        # First, make a HEAD request to check file size without downloading
        async with httpx.AsyncClient(timeout=30.0) as client:
            head_response = await client.head(
                api_url,
                headers=headers,
                follow_redirects=True
            )
            
            # Check content length if available
            content_length = head_response.headers.get("content-length")
            if content_length:
                file_size = int(content_length)
                max_size = 5 * 1024 * 1024  # 5MB in bytes
                
                if file_size > max_size:
                    size_mb = file_size / (1024 * 1024)
                    return [TextContent(
                        type="text",
                        text=f"⚠️ Cannot retrieve '{filename}' - File too large ({size_mb:.1f} MB)\n\n"
                             f"This tool has a 5MB maximum file size limit because large data files "
                             f"are not suitable for display in chat. For large datasets, please download "
                             f"the file directly from the Borealis website for analysis in statistical "
                             f"software or data analysis tools."
                    )]
            
            # Now download the actual file content
            response = await client.get(
                api_url,
                headers=headers,
                follow_redirects=True
            )
            
            # If we get a 401 or 403 with auth, try without auth for public files
            if response.status_code in [401, 403] and use_auth:
                headers = {}
                response = await client.get(
                    api_url,
                    headers=headers,
                    follow_redirects=True
                )
            
            # Check for error responses (HTML error pages, JSON errors)
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                # This is likely an error response
                try:
                    error_data = response.json()
                    if error_data.get("status") == "ERROR":
                        error_code = error_data.get("code", response.status_code)
                        if error_code == 403:
                            return [TextContent(
                                type="text",
                                text=f"🔒 Cannot access '{filename}' - File is restricted\n\n"
                                     f"This file requires specific access permissions that cannot be "
                                     f"granted through the API. To access restricted files, you may need to:\n"
                                     f"1. Request access from the dataset owner through the Borealis website\n"
                                     f"2. Verify you're affiliated with the authorized institution\n"
                                     f"3. Accept any terms of use or data use agreements"
                            )]
                        else:
                            return [TextContent(
                                type="text",
                                text=f"Error accessing file: {error_data.get('message', 'Unknown error')}"
                            )]
                except:
                    pass
            
            response.raise_for_status()
            
            # Get the file content
            file_content = response.content
            
            # Try to decode as text
            try:
                # Try UTF-8 first
                text_content = file_content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    # Try Latin-1 as fallback
                    text_content = file_content.decode('latin-1')
                except:
                    return [TextContent(
                        type="text",
                        text=f"⚠️ Cannot display '{filename}' - File appears to be binary or uses an unsupported encoding.\n\n"
                             f"This file cannot be decoded as text. It may be a binary file or use a non-standard "
                             f"text encoding. Please download it directly from Borealis to examine with appropriate software."
                    )]
            
            # Split into lines and check length
            lines = text_content.split('\n')
            total_lines = len(lines)
            
            # Format the output
            result_text = f"# File: {filename}\n\n"
            result_text += f"**File ID:** {file_id}\n"
            result_text += f"**Total lines:** {total_lines:,}\n"
            result_text += f"**File size:** {len(file_content):,} bytes ({len(file_content) / 1024:.1f} KB)\n\n"
            
            # Truncate if needed
            if total_lines > 100:
                result_text += f"⚠️ **Note:** File truncated to first 100 lines for display (file has {total_lines:,} total lines)\n\n"
                result_text += "---\n\n"
                display_lines = lines[:100]
            else:
                result_text += "---\n\n"
                display_lines = lines
            
            # Add line numbers and content
            for line_num, line in enumerate(display_lines, 1):
                # Limit very long lines
                if len(line) > 500:
                    line = line[:500] + "... (line truncated)"
                result_text += f"{line_num:4d} | {line}\n"
            
            if total_lines > 100:
                result_text += f"\n... ({total_lines - 100:,} more lines not shown)"
            
            return [TextContent(type="text", text=result_text)]
        
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            error_msg = f"File not found (ID: {file_id}). Please check the file ID from list_dataset_files."
        elif e.response.status_code == 403:
            error_msg = f"🔒 Access denied to '{filename}'. This file is restricted and requires special permissions."
        else:
            error_msg = f"HTTP error {e.response.status_code} while accessing file."
        return [TextContent(type="text", text=error_msg)]
    except httpx.RequestError as e:
        error_msg = f"Network error occurred: {str(e)}"
        return [TextContent(type="text", text=error_msg)]
    except Exception as e:
        error_msg = f"Unexpected error retrieving file: {str(e)}"
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