import re
from typing import List, Dict, Any, Optional

from src.preprocessing.chunking import DocumentChunker, ChunkConfig, SectionRecursiveConfig, SectionRecursiveStrategy

page_pattern = re.compile(r"--- Page (\d+) ---")

_worker_chunker: Optional[DocumentChunker] = None


def build_full_section_paths(sections: List[Dict[str, Any]]) -> List[str]:
    """
    Build the full hierarchical path for each section using the same
    heading-stack logic as the original serial implementation.
    """
    heading_stack = []
    full_section_paths = []

    for c in sections:
        # Determine current section level
        current_level = c.get('level', 1)

        # Determine current chapter number
        chapter_num = c.get('chapter', 0)

        # Pop sections that are deeper or siblings
        while heading_stack and heading_stack[-1][0] >= current_level:
            heading_stack.pop()
        
        # Push pair of (level, heading)
        if c['heading'] != "Introduction":
            heading_stack.append((current_level, c['heading']))

        # Construct section path
        path_list = [h[1] for h in heading_stack]
        full_section_path = " ".join(path_list)
        full_section_path = f"Chapter {chapter_num} " + full_section_path
        full_section_paths.append(full_section_path)

    return full_section_paths


def build_section_start_pages(sections: List[Dict[str, Any]]) -> List[int]:
    """
    Precompute the starting page for each section by scanning page markers
    in section order.
    """
    section_start_pages = []
    current_page = 1

    for c in sections:
        section_start_pages.append(current_page)

        markers = page_pattern.findall(c["content"])
        for marker in markers:
            current_page = int(marker) + 1

    return section_start_pages


def init_section_worker(chunk_config: ChunkConfig):
    """
    Initializer for multiprocessing workers. Creates one chunker per process.
    """
    if isinstance(chunk_config, SectionRecursiveConfig):
        strategy = SectionRecursiveStrategy(chunk_config)
    else:
        raise ValueError(f"Unsupported chunk config type: {type(chunk_config).__name__}")

    _worker_chunker = DocumentChunker(strategy)


def process_section(args) -> Dict[str, Any]:
    """
    Process one section independently.

    Returns chunk text, source list, and metadata without final chunk_id.
    Final chunk IDs should be assigned in the parent process during merge.
    """
    global _worker_chunker

    (
        c,
        full_section_path,
        current_page,
        markdown_file,
        chunk_mode,
        use_headings,
        chunk_config,
    ) = args

    chunker = _worker_chunker
    if chunker is None:
        if isinstance(chunk_config, SectionRecursiveConfig):
            strategy = SectionRecursiveStrategy(chunk_config)
        else:
            raise ValueError(f"Unsupported chunk config type: {type(chunk_config).__name__}")

        chunker = DocumentChunker(strategy)

    sub_chunks = chunker.chunk(c["content"])

    all_chunks = []
    sources = []
    metadata = []

    # Iterate through each chunk produced from this section
    for sub_chunk in sub_chunks:
        # Track all pages this specific chunk touches
        chunk_pages = set()

        # Split the sub_chunk by page markers to see if it
        # spans multiple pages.
        fragments = page_pattern.split(sub_chunk)

        if fragments[0].strip():
            chunk_pages.add(current_page)

        # Process the new pages found within this sub_chunk. 
        # Step by 2 where each pair represents (page number, text after it)
        for i in range(1, len(fragments), 2):
            try:
                # Get the new page number from the marker
                new_page = int(fragments[i]) + 1

                if fragments[i + 1].strip():
                    chunk_pages.add(new_page)

                current_page = new_page

            except (IndexError, ValueError):
                continue

        # Clean sub_chunk by removing page markers
        clean_chunk = page_pattern.sub("", sub_chunk).strip()

        # Skip introduction chunks for embedding
        if c["heading"] == "Introduction":
            continue

        chunk_prefix = (
            f"Description: {full_section_path} Content: "
            if use_headings else ""
        )

        # Prepare metadata
        meta = {
            "filename": markdown_file,
            "mode": chunk_mode,
            "char_len": len(clean_chunk),
            "word_len": len(clean_chunk.split()),
            "section": c["heading"],
            "section_path": full_section_path,
            "text_preview": clean_chunk[:100],
            "page_numbers": sorted(chunk_pages),
            # chunk_id assigned later in parent
        }

        all_chunks.append(chunk_prefix + clean_chunk)
        sources.append(markdown_file)
        metadata.append(meta)

    return {
        "all_chunks": all_chunks,
        "sources": sources,
        "metadata": metadata,
    }