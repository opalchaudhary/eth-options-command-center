from pathlib import Path

import streamlit as st


STYLE_FILES = [
    "styles/main.css",
    "styles/dashboard.css",
    "styles/cards.css",
    "styles/tables.css",
    "styles/sidebar.css",
    "styles/paper_trading.css",
]


def load_css():
    css_parts = []
    base_path = Path(__file__).parent

    for file_name in STYLE_FILES:
        css_path = base_path / file_name
        if css_path.exists():
            css_parts.append(css_path.read_text(encoding="utf-8"))

    if css_parts:
        css = "\n".join(css_parts)
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
