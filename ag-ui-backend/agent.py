import os
import json
import uuid
import asyncio
import random
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import re
from langgraph.graph import StateGraph, END, START
from langgraph.types import Command
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.runnables import RunnableConfig
from langchain_core.messages import SystemMessage, HumanMessage, ToolMessage, AIMessage

from copilotkit import CopilotKitState
from copilotkit.langgraph import (
    copilotkit_emit_message,
    copilotkit_emit_state,
    copilotkit_exit,
)

from langchain_google_genai import ChatGoogleGenerativeAI
from tavily import TavilyClient

load_dotenv()

# === Get Google model ===
def get_google_model(temperature=0.5):
    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=temperature,
        google_api_key=os.getenv("GOOGLE_API_KEY"),
        timeout=60,
    )

# === Agent State ===
class WeddingPlannerState(CopilotKitState):
    wedding_plan: Optional[Dict[str, Any]] = None
    planning_steps: Optional[List[Dict[str, Any]]] = None
    search_results: Optional[List[Dict[str, Any]]] = None
    last_search_query: Optional[str] = None

# === ✅ NEW: Function to extract multiple venues from article content ===
async def extract_venues_from_content(content: str) -> List[Dict]:
    model = get_google_model(temperature=0.3)
    system_prompt = """
You are an expert data extractor.
Given article content, strictly extract individual wedding venue entries as a JSON array, with no extra text or explanation.

Return a JSON array only, no preamble or postamble. Example:

[
  {
    "name": "Example Hall",
    "location": "Delhi",
    "capacity": "200-500 guests",
    "priceRange": "₹5000 - ₹10000",
    "rating": 4.5,
    "description": "Elegant hall with garden area",
    "image": "https://example.com/image.jpg",
    "phone": "Contact for details",
    "email": "Contact for details",
    "amenities": ["Parking", "Catering"],
    "features": ["Garden", "Stage"]
  }
]
If any field is missing, use "Contact for details".
Return valid JSON only.
"""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=content)
    ]

    try:
        response = await model.ainvoke(messages)
        text = response.content.strip()

        # Find first JSON array using regex
        match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
        if match:
            json_text = match.group(0)
            extracted = json.loads(json_text)
            if isinstance(extracted, list):
                return extracted

        print(f"Raw Gemini response (no valid JSON array found):\n{text}")

    except Exception as e:
        print(f"Error extracting venues: {e}")

    return []

# === Tavily Venue Search with venue extraction ===
async def search_tavily_venues(query: str, location: str = "", max_results: int = 6) -> List[Dict]:
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        print("Error: TAVILY_API_KEY not found in environment variables.")
        return get_mock_venues(query, location)

    tavily_client = TavilyClient(api_key=api_key)
    all_venues = []

    for attempt in range(3):
        try:
            search_query = f"wedding venues {query} {location}".strip()
            print(f"Attempt {attempt + 1}: Searching Tavily with query '{search_query}'")
            tavily_response = await asyncio.to_thread(
                tavily_client.search,
                search_query,
                search_depth="advanced",
                max_results=max_results
            )
            results = tavily_response.get("results", []) if tavily_response else []
            print(f"Tavily response: {len(results)} articles fetched.")

            for item in results[:max_results]:
                content = item.get("content", "")
                if not content.strip():
                    continue

                extracted_venues = await extract_venues_from_content(content)
                print(f"Extracted {len(extracted_venues)} venues from one article.")
                all_venues.extend(extracted_venues)

            if not all_venues:
                print(f"Warning: No venues extracted for query '{search_query}' on attempt {attempt + 1}. Falling back to mock venues.")
                all_venues = get_mock_venues(query, location)

            return all_venues

        except Exception as e:
            print(f"Error during Tavily search (attempt {attempt + 1}): {e}")
            if attempt == 2:
                print("All Tavily attempts failed. Falling back to mock venues.")
                return get_mock_venues(query, location)
            await asyncio.sleep(1)

def get_mock_venues(query: str, location: str) -> List[Dict]:
    mock_venues = [
        {
            "name": "Grand Palace Wedding Hall",
            "location": location or "Mumbai, Maharashtra",
            "capacity": "200-500 guests",
            "priceRange": "₹8,000 - ₹15,000",
            "rating": 4.8,
            "description": "Luxurious wedding hall with stunning architecture and world-class amenities",
            "image": "https://images.pexels.com/photos/169190/pexels-photo-169190.jpeg",
            "phone": "+91 98765 43210",
            "email": "events@grandpalace.com",
            "amenities": ["Valet Parking", "Catering Services", "Bridal Suite", "Audio/Visual Equipment"],
            "features": ["Grand Ballroom", "Garden Ceremony Area", "VIP Lounge", "Dance Floor"]
        },
    ]
    return random.sample(mock_venues, min(len(mock_venues), 1))

# === Entry Node ===
async def start_flow(state: WeddingPlannerState, config: RunnableConfig):
    print("Starting Wedding Planner Flow...")
    return state

# === Venue Search Node ===
async def search_venues_node(state: WeddingPlannerState, config: RunnableConfig):
    print("Starting venue search node with progress updates...")

    state["search_results"] = []

    try:
        model = get_google_model(temperature=0.3)

        SEARCH_VENUES_TOOL = {
            "type": "function",
            "function": {
                "name": "search_wedding_venues",
                "description": "Search for wedding venues based on user requirements",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {"type": "string"},
                        "query": {"type": "string"},
                        "guest_count": {"type": "string"},
                        "budget_range": {"type": "string"}
                    },
                    "required": ["query"]
                }
            }
        }

        system_prompt = """
You are a highly skilled wedding venue assistant. Your task is to help the user find the best wedding venues that match their needs.

When suggesting venues, always provide individual, specific venue listings only — avoid list articles, directories, or aggregator pages. Do not include "Top 50" or "Best venues" type links.

For each venue, use the following exact fields:

- name
- location
- capacity
- priceRange
- rating
- description
- image
- phone
- email
- amenities
- features

If you don't have exact data for a field, provide a reasonable default or "Contact for details".
"""

        model_with_tools = model.bind_tools([SEARCH_VENUES_TOOL])

        last_msg = None
        for msg in reversed(state["messages"]):
            if isinstance(msg, HumanMessage) and hasattr(msg, "content") and msg.content.strip():
                last_msg = msg
                break

        if not last_msg:
            await copilotkit_emit_message(config, "Please provide valid venue search criteria.")
            return Command(goto="general_chat_node", update=state)

        valid_messages = [last_msg]

        state["planning_steps"] = [
            {"step": "Extracting criteria from your message", "done": False},
            {"step": "Searching venues", "done": False},
            {"step": "Compiling results", "done": False},
        ]
        await copilotkit_emit_state(config, state)

        await asyncio.sleep(1)
        state["planning_steps"][0]["done"] = True
        await copilotkit_emit_state(config, state)

        response = await model_with_tools.ainvoke([
            SystemMessage(content=system_prompt),
            *valid_messages
        ], config)

        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_call = response.tool_calls[0]
            tool_args = tool_call.args if hasattr(tool_call, "args") else tool_call["args"]

            location = tool_args.get("location", "")
            query = tool_args.get("query", "")
            guest_count = tool_args.get("guest_count", "")
            budget_range = tool_args.get("budget_range", "")

            full_query = f"{query} {guest_count} {budget_range}".strip()
            state["last_search_query"] = full_query

        await asyncio.sleep(1)
        state["planning_steps"][1]["done"] = True
        await copilotkit_emit_state(config, state)

        venues = await search_tavily_venues(full_query, location)
        state["search_results"] = venues

        await asyncio.sleep(1)
        state["planning_steps"][2]["done"] = True
        await copilotkit_emit_state(config, state)

        if venues:
            await copilotkit_emit_message(config, f"Found {len(venues)} venues! Adding them now...")
        else:
            await copilotkit_emit_message(config, "I couldn't find venues, but I'll suggest some good mock options!")
            venues = get_mock_venues(query, location)
            state["search_results"] = venues

    except Exception as e:
        print(f"Error in search_venues_node: {e}")
        await copilotkit_emit_message(config, "Something went wrong. I'll show you some popular venue options.")
        venues = get_mock_venues("wedding venue", "")
        state["search_results"] = venues

    return Command(goto="add_venues_node", update=state)

# === Add Venues Node ===
async def add_venues_node(state: WeddingPlannerState, config: RunnableConfig):
    venues = state.get("search_results", [])

    if not venues:
        await copilotkit_emit_message(config, "No venues found. Please try again!")
        await copilotkit_exit(config)
        return Command(goto=END, update=state)

    venue_data_list = []
    for venue in venues:
        venue_data = {
            "name": venue["name"],
            "location": venue["location"],
            "capacity": venue["capacity"],
            "priceRange": venue["priceRange"],
            "rating": venue["rating"],
            "description": venue["description"],
            "image": venue.get("image", ""),
            "phone": venue.get("phone", "Contact for details"),
            "email": venue.get("email", "Contact for details"),
            "amenities": venue.get("amenities", ["Contact for details"]),
            "features": venue.get("features", ["Contact for details"])
        }
        venue_data_list.append(venue_data)

    add_venues_message = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{
            "id": str(uuid.uuid4()),
            "type": "function",
            "function": {
                "name": "addWeddingVenue",
                "arguments": json.dumps({"venues": venue_data_list})
            }
        }]
    }

    state["messages"] = [add_venues_message]

    await copilotkit_emit_message(config, f"✨ Added {len(venues)} venues! Click to explore details or ask for more options.")
    await copilotkit_exit(config)

    state["messages"] = []

    return Command(goto=END, update=state)

# === General Chat Node ===
async def general_chat_node(state: WeddingPlannerState, config: RunnableConfig):
    try:
        model = get_google_model(temperature=0.7)
        system_prompt = "You are a wedding venue assistant. Help users with venue search, questions, or suggestions."

        valid_messages = []
        for msg in state["messages"]:
            if hasattr(msg, "content") and msg.content.strip():
                valid_messages.append(msg)
        if not valid_messages:
            await copilotkit_emit_message(config, "Please provide valid input or ask about wedding venues.")
            await copilotkit_exit(config)
            return Command(goto=END, update=state)

        for attempt in range(3):
            try:
                response = await asyncio.wait_for(
                    model.ainvoke([
                        SystemMessage(content=system_prompt),
                        *valid_messages
                    ], config),
                    timeout=60.0
                )
                break
            except asyncio.TimeoutError:
                if attempt == 2:
                    raise Exception("Model invocation failed after retries")

        if hasattr(response, "content") and response.content.strip():
            await copilotkit_emit_message(config, response.content)

        state["messages"] += [response]

        await copilotkit_exit(config)
        return Command(goto=END, update=state)

    except Exception as e:
        await copilotkit_emit_message(config, "I'm here to help you find the perfect venue! Please share your preferences.")
        await copilotkit_exit(config)
        return Command(goto=END, update=state)

# === Routing Logic ===
def route_user_intent(state: WeddingPlannerState) -> str:
    if not state.get("messages"):
        return "general_chat_node"

    last_message = state["messages"][-1]
    if hasattr(last_message, "content"):
        content = last_message.content.lower()
        search_keywords = [
            "find", "search", "look", "show", "venue", "hall", "location",
            "budget", "guest", "capacity", "wedding", "reception", "ceremony",
            "luxury", "cheap", "affordable", "outdoor", "indoor", "garden"
        ]

        if any(keyword in content for keyword in search_keywords):
            return "search_venues_node"

    return "general_chat_node"

# === Graph Setup ===
workflow = StateGraph(WeddingPlannerState)
workflow.add_node("start_flow", start_flow)
workflow.add_node("search_venues_node", search_venues_node)
workflow.add_node("add_venues_node", add_venues_node)
workflow.add_node("general_chat_node", general_chat_node)

workflow.set_entry_point("start_flow")
workflow.add_edge(START, "start_flow")

workflow.add_conditional_edges(
    "start_flow",
    route_user_intent,
    {
        "search_venues_node": "search_venues_node",
        "general_chat_node": "general_chat_node"
    }
)

workflow.add_edge("search_venues_node", "add_venues_node")
workflow.add_edge("add_venues_node", END)
workflow.add_edge("general_chat_node", END)

wedding_graph = workflow.compile(checkpointer=MemorySaver())
