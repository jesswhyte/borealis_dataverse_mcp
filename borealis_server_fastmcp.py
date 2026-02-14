#!/usr/bin/env python3
"""
Borealis Dataverse MCP Server - FastMCP Version
Supports both stdio (local) and HTTP (Railway deployment) transports
"""
import os
import sys
import httpx
from typing import Optional
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.requests import Request

# Configuration
BOREALIS_BASE_URL = "https://borealisdata.ca/api"
API_KEY = os.environ.get("BOREALIS_API_KEY", "")

# Token authentication for HTTP mode (Railway deployment)
# Tokens should be set as environment variable: AUTH_TOKENS=token1,token2,token3
AUTH_TOKENS = set(os.environ.get("AUTH_TOKENS", "").split(",")) if os.environ.get("AUTH_TOKENS") else set()


# Authentication middleware for HTTP mode
class TokenAuthMiddleware(BaseHTTPMiddleware):
    """Middleware to validate Bearer tokens for HTTP requests."""

    async def dispatch(self, request: Request, call_next):
        # Skip auth for health check endpoints
        if request.url.path in ["/health", "/", "/sse"]:
            return await call_next(request)

        # Check for Authorization header
        auth_header = request.headers.get("Authorization", "")

        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"error": "Missing or invalid Authorization header. Use: Authorization: Bearer <token>"}
            )

        # Extract token
        token = auth_header[7:]  # Remove "Bearer " prefix

        # Validate token
        if token not in AUTH_TOKENS:
            return JSONResponse(
                status_code=403,
                content={"error": "Invalid authentication token"}
            )

        # Token is valid, proceed with request
        return await call_next(request)


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

# Create FastMCP server instance
mcp = FastMCP("borealis-dataverse")

# Utility functions
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

# Tool implementations with FastMCP decorators

@mcp.tool()
async def search_datasets(
    query: str,
    per_page: int = 10,
    sort: str = "relevance",
    type: Optional[str] = None,
    dataverse: Optional[str] = None,
    country: Optional[str] = None,
    province: Optional[str] = None,
    city: Optional[str] = None
) -> str:
    """
    Search for datasets in Borealis Dataverse by keywords, subjects, or other criteria.

    Returns a list of matching datasets with basic information. When presenting results to the user,
    ALWAYS include the full DOI URL (e.g., https://doi.org/10.34990/FK2/XXXXX) and the authors for
    each dataset in your summary. Do not omit author information.

    Args:
        query: Search query string (searches titles, descriptions, keywords, etc.)
        per_page: Number of results per page (default: 10, max: 100)
        sort: Sort order: 'name' (alphabetical), 'date' (newest first), or 'relevance' (default)
        type: Filter by type: 'dataset', 'dataverse', or 'file'. Leave empty for all types.
        dataverse: Optional: Limit search to a specific university/institution dataverse.
                   Can be specified as either the university name (e.g., 'University of Toronto', 'UBC')
                   or the dataverse identifier (e.g., 'toronto', 'ubc'). Supports all major Canadian
                   universities and colleges.
        country: Optional: Filter by the geographic coverage/subject area of datasets (e.g., datasets
                 ABOUT 'Canada', 'United States'). This indicates what region the data describes,
                 not where researchers are located.
        province: Optional: Filter by the geographic coverage/subject area of datasets (e.g., datasets
                  ABOUT 'Ontario', 'Nova Scotia', 'British Columbia', 'Quebec'). This indicates what
                  province/state the data describes, not where researchers are located.
        city: Optional: Filter by the geographic coverage/subject area of datasets (e.g., datasets
              ABOUT 'Toronto', 'Halifax', 'Vancouver'). This indicates what city the data describes,
              not where researchers are located.
    """
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
    if sort != "relevance":
        params["sort"] = sort
        params["order"] = "desc" if sort == "date" else "asc"

    # Add type filter if specified
    if type:
        params["type"] = type

    # Add dataverse/subtree filter if specified
    if dataverse:
        params["subtree"] = dataverse

    # Add geographic filters using fq (filter query) parameter
    if country:
        params["fq"] = f"country:{country}"

    if province:
        fq_value = f"state:{province}"
        if "fq" in params:
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
    if API_KEY and len(API_KEY) > 10:
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
                headers = {}
                response = await client.get(
                    f"{BOREALIS_BASE_URL}/search",
                    params=params,
                    headers=headers
                )

            response.raise_for_status()
            data = response.json()

        # Check if the response was successful
        if data.get("status") != "OK":
            return f"Error: API returned status '{data.get('status')}'"

        # Extract search results
        search_data = data.get("data", {})
        items = search_data.get("items", [])
        total_count = search_data.get("total_count", 0)

        if total_count == 0:
            return f"No results found for query: '{query}'"

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
                    result_text += f"   DOI: {url}\n"

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

        return result_text

    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP error occurred: {e.response.status_code}\n"
        try:
            error_data = e.response.json()
            error_msg += f"API Response: {error_data}\n"
        except:
            error_msg += f"Response: {e.response.text}\n"
        error_msg += "\nNote: Searches of published datasets don't require authentication."
        return error_msg
    except httpx.RequestError as e:
        return f"Request error occurred: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


@mcp.tool()
async def get_dataset_metadata(identifier: str) -> str:
    """
    Retrieve detailed metadata for a specific dataset from Borealis Dataverse.

    Use this when the user asks for more information about a specific dataset found in search results.
    Returns comprehensive metadata including full description, authors, keywords, subjects, file
    information, and more.

    Args:
        identifier: Dataset identifier - can be either a DOI (e.g., 'doi:10.34990/FK2/ABC123' or
                    'https://doi.org/10.34990/FK2/ABC123') or a numeric database ID. DOIs are preferred.
    """
    if not identifier:
        return "Error: No dataset identifier provided."

    # Clean up the identifier
    if identifier.startswith("http"):
        identifier = identifier.split("doi.org/")[-1]
        if not identifier.startswith("doi:"):
            identifier = f"doi:{identifier}"
    elif not identifier.startswith("doi:") and not identifier.isdigit():
        identifier = f"doi:{identifier}"

    # Build the API URL
    if identifier.startswith("doi:"):
        api_url = f"{BOREALIS_BASE_URL}/datasets/:persistentId/metadata"
        params = {"persistentId": identifier}
    else:
        api_url = f"{BOREALIS_BASE_URL}/datasets/{identifier}/metadata"
        params = {}

    # Prepare headers
    headers = {"Accept": "application/ld+json"}
    use_auth = False
    if API_KEY and len(API_KEY) > 10:
        headers["X-Dataverse-key"] = API_KEY
        use_auth = True

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(api_url, params=params, headers=headers)

            # If we get a 401 with auth, try again without auth for public datasets
            if response.status_code == 401 and use_auth:
                headers = {"Accept": "application/ld+json"}
                response = await client.get(api_url, params=params, headers=headers)

            response.raise_for_status()
            response_data = response.json()

        # Check if response was successful
        if response_data.get("status") != "OK":
            return f"Error: API returned status '{response_data.get('status')}'"

        # Extract the actual metadata from the 'data' field
        metadata = response_data.get("data", {})

        if not metadata:
            return "Error: No metadata found in API response."

        # Parse and format the metadata
        result_text = "# Dataset Metadata\n\n"

        # Title
        title = metadata.get("title", metadata.get("schema:name", "No title available"))
        result_text += f"**Title:** {title}\n\n"

        # DOI/Identifier
        doi = metadata.get("@id", "")
        if doi:
            result_text += f"**DOI:** {doi}\n\n"

        # Description
        description_obj = metadata.get("citation:dsDescription", {})
        if isinstance(description_obj, dict):
            description = description_obj.get("citation:dsDescriptionValue", "No description available")
        else:
            description = metadata.get("schema:description", "No description available")

        # Strip HTML tags for cleaner display
        import re
        description_text = re.sub(r'<[^>]+>', '', description)
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

        # Alternative URL
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

        return result_text

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
        return error_msg
    except httpx.RequestError as e:
        return f"Request error occurred: {str(e)}"
    except Exception as e:
        return f"Unexpected error retrieving metadata: {str(e)}"


@mcp.tool()
async def list_dataset_files(
    identifier: str,
    limit: int = 20,
    offset: int = 0,
    file_type: Optional[str] = None
) -> str:
    """
    List all files in a specific dataset from Borealis Dataverse.

    Use this when the user asks what files are in a dataset or wants to see the file list.
    Returns file names, sizes, formats, descriptions, and access restrictions. By default shows
    first 20 files; user can request more or filter by file type.

    Args:
        identifier: Dataset identifier - can be either a DOI (e.g., 'doi:10.34990/FK2/ABC123' or
                    'https://doi.org/10.34990/FK2/ABC123') or a numeric database ID. DOIs are preferred.
        limit: Maximum number of files to return (default: 20). Use higher values if user wants to see more files.
        offset: Number of files to skip (for pagination). Use to show additional files beyond the initial results.
        file_type: Optional: Filter by file type or search in filenames (e.g., 'csv', 'readme', 'pdf',
                   'spss', 'data'). Searches in both filename and friendly type.
    """
    if not identifier:
        return "Error: No dataset identifier provided."

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
            response = await client.get(api_url, params=params, headers=headers)

            # If we get a 401 with auth, try again without auth for public datasets
            if response.status_code == 401 and use_auth:
                headers = {}
                response = await client.get(api_url, params=params, headers=headers)

            response.raise_for_status()
            response_data = response.json()

        # Check if response was successful
        if response_data.get("status") != "OK":
            return f"Error: API returned status '{response_data.get('status')}'"

        # Extract files from the response
        files = response_data.get("data", [])
        total_count = response_data.get("totalCount", len(files))

        # Apply client-side filtering if file_type is specified
        if file_type:
            filter_lower = file_type.lower()
            filtered_files = []
            for file_info in files:
                data_file = file_info.get("dataFile", {})
                filename = data_file.get("filename", "").lower()
                friendly_type = data_file.get("friendlyType", "").lower()

                if filter_lower in filename or filter_lower in friendly_type:
                    filtered_files.append(file_info)

            files = filtered_files

        if not files:
            filter_msg = f" matching '{file_type}'" if file_type else ""
            return f"No files found{filter_msg} in this dataset."

        # Format the file list
        result_text = f"# Dataset Files\n\n"

        if file_type:
            result_text += f"**Showing:** Files matching '{file_type}'\n"
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
        if not file_type and total_count > (offset + len(files)):
            remaining = total_count - (offset + len(files))
            result_text += f"\n*There are {remaining} more file(s) in this dataset. "
            result_text += f"To see more, ask to show the next files or specify a larger limit.*\n"

        return result_text

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"Dataset not found: {identifier}\nPlease check the DOI or dataset ID and try again."
        else:
            error_msg = f"HTTP error occurred: {e.response.status_code}\n"
            try:
                error_data = e.response.json()
                error_msg += f"API Response: {error_data}\n"
            except:
                error_msg += f"Response: {e.response.text}\n"
            return error_msg
    except httpx.RequestError as e:
        return f"Request error occurred: {str(e)}"
    except Exception as e:
        return f"Unexpected error listing files: {str(e)}"


@mcp.tool()
async def get_dataset_file(file_id: str, filename: str = "file") -> str:
    """
    Download and retrieve the content of a specific file from a Borealis dataset.

    Use this when the user wants to examine, analyze, or explore a specific file.
    IMPORTANT: Only supports text-based files under 5MB. Binary files (PDF, ZIP, Excel) and large
    data files are not suitable for chat display. The file content will be truncated to 100 lines
    if longer.

    Args:
        file_id: The numeric file ID from the file list (e.g., '276461'). Get this from list_dataset_files.
        filename: Optional: The filename for context (helps with user-friendly error messages).
    """
    if not file_id:
        return "Error: No file ID provided. Use list_dataset_files to get file IDs."

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
    is_binary = any(filename_lower.endswith(ext) for ext in BINARY_EXTENSIONS)

    if is_binary:
        return (f"⚠️ Cannot retrieve '{filename}' - Binary file format not supported.\n\n"
                f"This tool only supports text-based files (CSV, TXT, DAT, R, Python, etc.) "
                f"that can be displayed in chat. Binary files like PDF, Excel, ZIP, and SPSS "
                f"data files (.sav) must be downloaded separately through the Borealis website.")

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
            head_response = await client.head(api_url, headers=headers, follow_redirects=True)

            # Check content length if available
            content_length = head_response.headers.get("content-length")
            if content_length:
                file_size = int(content_length)
                max_size = 5 * 1024 * 1024  # 5MB in bytes

                if file_size > max_size:
                    size_mb = file_size / (1024 * 1024)
                    return (f"⚠️ Cannot retrieve '{filename}' - File too large ({size_mb:.1f} MB)\n\n"
                            f"This tool has a 5MB maximum file size limit because large data files "
                            f"are not suitable for display in chat. For large datasets, please download "
                            f"the file directly from the Borealis website for analysis in statistical "
                            f"software or data analysis tools.")

            # Now download the actual file content
            response = await client.get(api_url, headers=headers, follow_redirects=True)

            # If we get a 401 or 403 with auth, try without auth for public files
            if response.status_code in [401, 403] and use_auth:
                headers = {}
                response = await client.get(api_url, headers=headers, follow_redirects=True)

            # Check for error responses
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                try:
                    error_data = response.json()
                    if error_data.get("status") == "ERROR":
                        error_code = error_data.get("code", response.status_code)
                        if error_code == 403:
                            return (f"🔒 Cannot access '{filename}' - File is restricted\n\n"
                                    f"This file requires specific access permissions that cannot be "
                                    f"granted through the API. To access restricted files, you may need to:\n"
                                    f"1. Request access from the dataset owner through the Borealis website\n"
                                    f"2. Verify you're affiliated with the authorized institution\n"
                                    f"3. Accept any terms of use or data use agreements")
                        else:
                            return f"Error accessing file: {error_data.get('message', 'Unknown error')}"
                except:
                    pass

            response.raise_for_status()

            # Get the file content
            file_content = response.content

            # Try to decode as text
            try:
                text_content = file_content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    text_content = file_content.decode('latin-1')
                except:
                    return (f"⚠️ Cannot display '{filename}' - File appears to be binary or uses an unsupported encoding.\n\n"
                            f"This file cannot be decoded as text. It may be a binary file or use a non-standard "
                            f"text encoding. Please download it directly from Borealis to examine with appropriate software.")

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

            return result_text

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return f"File not found (ID: {file_id}). Please check the file ID from list_dataset_files."
        elif e.response.status_code == 403:
            return f"🔒 Access denied to '{filename}'. This file is restricted and requires special permissions."
        else:
            return f"HTTP error {e.response.status_code} while accessing file."
    except httpx.RequestError as e:
        return f"Network error occurred: {str(e)}"
    except Exception as e:
        return f"Unexpected error retrieving file: {str(e)}"


# Main entry point with hybrid transport support
if __name__ == "__main__":
    # Check environment variable for transport mode
    transport_mode = os.environ.get("MCP_TRANSPORT", "auto")

    # Auto-detect: if stdin is not a TTY, we're likely being run via stdio by Claude Desktop
    if transport_mode == "auto":
        if not sys.stdin.isatty():
            transport_mode = "stdio"
        else:
            transport_mode = "http"

    if transport_mode == "stdio":
        # STDIO mode - for Claude Desktop local use
        print("Starting Borealis MCP server in stdio mode...", file=sys.stderr)
        mcp.run(transport="stdio")
    else:
        # HTTP mode - for Railway deployment or local HTTP testing
        port = int(os.environ.get("PORT", 8000))
        host = os.environ.get("HOST", "0.0.0.0")
        print(f"Starting Borealis MCP server in HTTP mode on {host}:{port}...", file=sys.stderr)

        # Authentication status
        if AUTH_TOKENS:
            print(f"✓ Authentication enabled with {len(AUTH_TOKENS)} token(s)", file=sys.stderr)
            print("⚠️  Note: Authentication middleware is defined but not yet integrated.", file=sys.stderr)
        else:
            print("⚠️  WARNING: No authentication tokens configured!", file=sys.stderr)
            print("   Set AUTH_TOKENS environment variable for production use.", file=sys.stderr)

        print(f"Starting server on {host}:{port}...", file=sys.stderr)
        sys.stderr.flush()  # Ensure logs are visible immediately

        # Use ASGI app approach (mcp.run() doesn't support HTTP transport)
        # Create ASGI app and run with uvicorn directly
        import uvicorn

        print("Creating FastMCP ASGI app...", file=sys.stderr)
        sys.stderr.flush()

        app = mcp.http_app()

        print("Starting uvicorn server...", file=sys.stderr)
        sys.stderr.flush()

        uvicorn.run(app, host=host, port=port, log_level="info")
