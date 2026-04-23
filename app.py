import asyncio
import base64
import csv
import email
import io
import os
import sys
from email import policy
from urllib.parse import parse_qs, urlparse

import streamlit as st
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

UTM_PARAMS = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"]


def get_html_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")
    else:
        if msg.get_content_type() == "text/html":
            payload = msg.get_payload(decode=True)
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return None


def get_label(tag):
    text = " ".join(tag.get_text(separator=" ").split()).strip()
    if text:
        return text
    if tag.get("title", "").strip():
        return tag["title"].strip()
    alts = [i.get("alt", "").strip() for i in tag.find_all("img") if i.get("alt", "").strip()]
    if alts:
        return " | ".join(alts)
    if tag.get("aria-label", "").strip():
        return tag["aria-label"].strip()
    return "[image link]"


def extract_raw_links(html):
    soup = BeautifulSoup(html, "lxml")
    seen = set()
    links = []
    counter = 0
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href.startswith("http"):
            continue
        if href in seen:
            continue
        seen.add(href)
        counter += 1
        links.append({"num": counter, "label": get_label(tag), "href": href})
    return links


def decode_px_show(url):
    parsed = urlparse(url)
    if "PX-Show" not in parsed.path:
        return url
    params = parse_qs(parsed.query)
    if "url" not in params:
        return url
    try:
        encoded = params["url"][0] + "=="
        decoded = base64.b64decode(encoded).decode("utf-8", errors="replace")
        return decoded
    except Exception:
        return url


def check_utms(url):
    params = parse_qs(urlparse(url).query)
    return {p: params[p][0] if p in params else None for p in UTM_PARAMS}


async def resolve_all(links, progress_bar, status_text):
    results = []
    total = len(links)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        page = await context.new_page()

        for link in links:
            num = link["num"]
            label = link["label"]
            href = link["href"]

            status_text.text(f"Resolving {num}/{total}: {label[:60]}")
            progress_bar.progress(num / total)

            try:
                await page.goto(href, wait_until="domcontentloaded", timeout=15000)
                raw_final = page.url
                final_url = decode_px_show(raw_final)
                error = ""
            except Exception as e:
                final_url = href
                error = str(e)

            utm_vals = check_utms(final_url)
            all_present = all(v is not None for v in utm_vals.values())

            row = {
                "Link #": num,
                "Link Label": label,
                "Tracking URL": href,
                "Final URL": final_url,
                "Redirect Error": error,
                "UTM Status": "PASS" if (not error and all_present) else "FAIL",
            }
            for p_name in UTM_PARAMS:
                row[p_name] = utm_vals[p_name] or ""

            results.append(row)

        await browser.close()

    return results


def results_to_csv_bytes(rows):
    fieldnames = [
        "Link #", "Link Label", "Tracking URL", "Final URL",
        "Redirect Error", "UTM Status",
    ] + UTM_PARAMS

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


# ── Streamlit UI ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="UTM QA Tool", page_icon="🔗", layout="centered")

st.title("🔗 UTM QA Tool")
st.caption("Upload an .eml file to extract all links, resolve redirects, and check UTM parameters.")

uploaded_file = st.file_uploader("Upload your .eml file", type=["eml"])

if uploaded_file:
    msg = email.message_from_bytes(uploaded_file.read(), policy=policy.compat32)
    html = get_html_body(msg)

    if not html:
        st.error("No HTML body found in this .eml file.")
    else:
        links = extract_raw_links(html)
        st.success(f"Found **{len(links)} links** in the email.")

        if st.button("Run UTM QA", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()

            with st.spinner("Launching browser and resolving redirects..."):
                results = asyncio.run(resolve_all(links, progress_bar, status_text))

            progress_bar.progress(1.0)
            status_text.text("Done!")

            passes = sum(1 for r in results if r["UTM Status"] == "PASS")
            fails  = sum(1 for r in results if r["UTM Status"] == "FAIL")

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Links", len(results))
            col2.metric("PASS", passes)
            col3.metric("FAIL", fails)

            st.dataframe(results, use_container_width=True)

            csv_bytes = results_to_csv_bytes(results)
            filename = os.path.splitext(uploaded_file.name)[0] + "_links.csv"
            st.download_button(
                label="⬇️ Download CSV",
                data=csv_bytes,
                file_name=filename,
                mime="text/csv",
            )