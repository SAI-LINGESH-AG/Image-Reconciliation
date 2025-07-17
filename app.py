import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import boto3
from io import StringIO, BytesIO

# -------------------- Adaptive Styling --------------------
def style_app():
    st.markdown("""
        <style>
            html, body, [class*="css"] {
                font-family: 'Segoe UI', sans-serif;
            }

            /* Hide Streamlit Header, Footer, Menu */
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;} 

            /* Common layout and scroll behavior */
            .scrollable-table div[data-testid="stDataFrame"] {
                overflow-x: auto;
                overflow-y: auto;
                max-height: 300px;
                white-space: normal;
                border-radius: 10px;
            }

            .stButton>button {
                font-size: 18px !important;
                padding: 10px 20px;
                border-radius: 10px;
                border: none;
                transition: background-color 0.3s ease;
            }

            /* Light Mode Theme */
            @media (prefers-color-scheme: light) {
                .header-title {
                    font-size: 48px;
                    color: #003366;
                    font-weight: bold;
                    text-align: left;
                    margin-top: -40px;
                }
                .subtitle {
                    font-size: 32px;
                    color: #004080;
                    text-align: center;
                    margin-top: -30px;
                    margin-bottom: 30px;
                }
                .stButton>button {
                    background-color: #0072C6;
                    color: white;
                }
                .stButton>button:hover {
                    background-color: #005a9e;
                }
            }

            /* Dark Mode Theme */
            @media (prefers-color-scheme: dark) {
                .header-title {
                    font-size: 48px;
                    color: #aad8ff;
                    font-weight: bold;
                    text-align: left;
                    margin-top: -40px;
                }
                .subtitle {
                    font-size: 32px;
                    color: #66aaff;
                    text-align: center;
                    margin-top: -30px;
                    margin-bottom: 30px;
                }
                .stButton>button {
                    background-color: #3399ff;
                    color: black;
                }
                .stButton>button:hover {
                    background-color: #0077cc;
                    color: white;
                }
            }
        </style>
    """, unsafe_allow_html=True)

def show_header():
    st.markdown("<div class='header-title'>AMGEN</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Image Metadata Reconciliation</div>", unsafe_allow_html=True)

# -------------------- File Parsing --------------------
def parse_metadata(file_content, filetype, filename):
    try:
        if filetype == "CSV":
            return pd.read_csv(StringIO(file_content.decode('utf-8')))
        elif filetype == "JSON":
            return pd.read_json(StringIO(file_content.decode('utf-8')))
        elif filetype == "XML":
            tree = ET.parse(BytesIO(file_content))
            root = tree.getroot()
            rows = []
            for child in root:
                row = {}
                for sub in child:
                    row[sub.tag] = sub.text
                rows.append(row)
            return pd.DataFrame(rows)
        else:
            st.warning("Unsupported file format.")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to parse file {filename}: {e}")
        return pd.DataFrame()

# -------------------- S3 File Fetch --------------------
def fetch_s3_file(access_key, secret_key, session_token, bucket_name, file_key):
    try:
        if not bucket_name or not file_key:
            raise ValueError("Bucket name or file key is empty")
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token
        )
        s3 = session.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        return response['Body'].read(), file_key
    except Exception as e:
        st.error(f"Failed to fetch file from S3: {e}")
        return None, None

# -------------------- S3 File List --------------------
def list_s3_files(access_key, secret_key, session_token, bucket_name):
    try:
        if not bucket_name:
            raise ValueError("Bucket name is empty")
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token
        )
        s3 = session.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if obj['Key'].split('.')[-1].lower() in ['csv', 'json', 'xml']]
        return []
    except Exception as e:
        st.error(f"Failed to list files in S3 bucket: {e}")
        return []

# -------------------- Primary Key Detection --------------------
def detect_primary_key(df):
    for column in df.columns:
        if df[column].is_unique and df[column].notnull().all():
            return column
    return df.columns[0]

# -------------------- Matcher --------------------
def match_data(meta_df, cust_df, meta_key, cust_key):
    merged = pd.merge(meta_df, cust_df, left_on=meta_key, right_on=cust_key, how='outer', indicator=True)
    matched = merged[merged['_merge'] == 'both']
    unmatched = merged[merged['_merge'] != 'both']
    return matched, unmatched

# -------------------- Clean Headers --------------------
def clean_column_headers(df):
    df.columns = [col.replace('_', ' ').title() for col in df.columns]
    return df

# -------------------- Home Page --------------------
def home_page():
    style_app()
    show_header()

    st.markdown("### Welcome to the Image Metadata Reconciliation Tool")

    st.markdown("""
    A smart solution to automatically reconcile image metadata with customer records — helping you unlock insights faster and make confident business decisions.
    """)

    # Light blue box styling (consistent across light/dark themes)
    box_style = """
        <style>
        .info-box {
            background-color: #e6f2ff;
            color: #003366;
            border-radius: 12px;
            padding: 20px;
            margin-top: 10px;
            margin-bottom: 10px;
            border: 1px solid #b3d9ff;
            box-shadow: 0 4px 12px rgba(0, 102, 204, 0.05);
        }

        .info-box ul {
            padding-left: 20px;
        }

        .info-box h4 {
            margin-top: 0;
        }
        </style>
    """
    st.markdown(box_style, unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        <div class="info-box">
            <h4>Key Benefits</h4>
            <ul>
                <li>Connects cloud metadata with enterprise data</li>
                <li>Automatically detects and matches unique identifiers</li>
                <li>Visualizes matched vs unmatched records</li>
                <li>Minimizes manual work and errors</li>
                <li>Secure, no data stored or shared</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="info-box">
            <h4>How It Works</h4>
            <ul>
                <li>Click <strong>Get Started</strong></li>
                <li>Enter your S3 credentials for metadata</li>
                <li>Enter your S3 credentials for customer data</li>
                <li>Choose the metadata and customer files</li>
                <li>Instantly view reconciliation results</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    st.markdown(" ")

    if st.button("Get Started", use_container_width=True):
        st.session_state.page = "upload"

# -------------------- Upload Page --------------------
def upload_page():
    style_app()
    show_header()
    st.markdown("### Upload Metadata & Customer Data")

    col1, col2 = st.columns(2)

    # ----------- Column 1: Metadata Source (S3) -----------
    with col1:
        st.subheader("Metadata Source (S3)")
        st.text_input("Metadata AWS Access Key ID", key="aws_access_key")
        st.text_input("Metadata AWS Secret Access Key", type="password", key="aws_secret_key")
        st.text_input("Metadata AWS Session Token", type="password", key="aws_session_token")
        st.text_input("Metadata S3 Bucket Name", key="bucket_name")

        file_options = []
        if all(st.session_state.get(k) for k in ["aws_access_key", "aws_secret_key", "aws_session_token", "bucket_name"]):
            file_options = list_s3_files(
                st.session_state.aws_access_key,
                st.session_state.aws_secret_key,
                st.session_state.aws_session_token,
                st.session_state.bucket_name
            )

        if file_options:
            st.selectbox("Select Metadata File", options=file_options, key="file_key")
        else:
            st.selectbox("Select Metadata File", options=["No files available"], key="file_key", disabled=True)

    # ----------- Column 2: Customer Data (S3) -----------
    with col2:
        st.subheader("Customer Data (S3)")
        st.text_input("Customer AWS Access Key ID", key="cust_aws_access_key")
        st.text_input("Customer AWS Secret Access Key", type="password", key="cust_aws_secret_key")
        st.text_input("Customer AWS Session Token", type="password", key="cust_aws_session_token")
        st.text_input("Customer S3 Bucket Name", key="cust_bucket_name")

        cust_file_options = []
        if all(st.session_state.get(k) for k in ["cust_aws_access_key", "cust_aws_secret_key", "cust_aws_session_token", "cust_bucket_name"]):
            cust_file_options = list_s3_files(
                st.session_state.cust_aws_access_key,
                st.session_state.cust_aws_secret_key,
                st.session_state.cust_aws_session_token,
                st.session_state.cust_bucket_name
            )

        if cust_file_options:
            st.selectbox("Select Customer File", options=cust_file_options, key="cust_file_key")
        else:
            st.selectbox("Select Customer File", options=["No files available"], key="cust_file_key", disabled=True)

    st.markdown(" ")

    if st.button("Start Parsing & Matching", use_container_width=True):
        required_keys = ["aws_access_key", "aws_secret_key", "aws_session_token", "bucket_name", "file_key",
                         "cust_aws_access_key", "cust_aws_secret_key", "cust_aws_session_token", "cust_bucket_name", "cust_file_key"]
        if all(st.session_state.get(k) for k in required_keys) and st.session_state.get("file_key") != "No files available" and st.session_state.get("cust_file_key") != "No files available":
            st.session_state.page = "results"
        else:
            st.error("Please complete all fields and select valid files.")

    if st.button("Home", use_container_width=True):
        reset_session()

# -------------------- Results Page --------------------
def results_page():
    # Guard clause to prevent processing if page is not 'results'
    if st.session_state.get("page") != "results":
        st.session_state.page = "home"
        st.rerun()
        return

    style_app()
    show_header()
    st.markdown("### Results - Matched and Unmatched Records")

    # Validate session state before S3 calls
    required_keys = ["aws_access_key", "aws_secret_key", "aws_session_token", "bucket_name", "file_key",
                     "cust_aws_access_key", "cust_aws_secret_key", "cust_aws_session_token", "cust_bucket_name", "cust_file_key"]
    if not all(st.session_state.get(k) for k in required_keys) or st.session_state.get("file_key") == "No files available" or st.session_state.get("cust_file_key") == "No files available":
        st.error("Invalid or missing session state. Returning to home page.")
        reset_session()
        return

    # Metadata S3
    file_content, filename = fetch_s3_file(
        st.session_state.aws_access_key,
        st.session_state.aws_secret_key,
        st.session_state.aws_session_token,
        st.session_state.bucket_name,
        st.session_state.file_key
    )

    filetype = filename.split(".")[-1].upper() if filename else None
    metadata_df = parse_metadata(file_content, filetype, filename) if file_content and filetype else pd.DataFrame()

    # Customer Data (S3)
    cust_file_content, cust_filename = fetch_s3_file(
        st.session_state.cust_aws_access_key,
        st.session_state.cust_aws_secret_key,
        st.session_state.cust_aws_session_token,
        st.session_state.cust_bucket_name,
        st.session_state.cust_file_key
    )
    cust_filetype = cust_filename.split(".")[-1].upper() if cust_filename else None
    customer_df = parse_metadata(cust_file_content, cust_filetype, cust_filename) if cust_file_content else pd.DataFrame()

    if not metadata_df.empty and not customer_df.empty:
        meta_key = detect_primary_key(metadata_df)
        cust_key = detect_primary_key(customer_df)

        col1, col2 = st.columns(2)
        with col1:
            st.success(f"Detected Metadata Key: `{meta_key}`")
        with col2:
            st.success(f"Detected Customer Key: `{cust_key}`")

        matched_df, unmatched_df = match_data(metadata_df, customer_df, meta_key, cust_key)

        matched_df = clean_column_headers(matched_df.drop(columns=["_merge"]))
        unmatched_df = clean_column_headers(unmatched_df.drop(columns=["_merge"]))

        st.subheader(f"Matched Records (Count: {len(matched_df)})")
        with st.container():
            st.markdown("<div class='scrollable-table'>", unsafe_allow_html=True)
            st.dataframe(matched_df, use_container_width=True, height=300)
            st.markdown("</div>", unsafe_allow_html=True)

        st.subheader(f"Unmatched Records (Count: {len(unmatched_df)})")
        with st.container():
            st.markdown("<div class='scrollable-table'>", unsafe_allow_html=True)
            st.dataframe(unmatched_df, use_container_width=True, height=300)
            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.warning("No data found or parsing failed.")

    if st.button("Home", use_container_width=True):
        reset_session()

# -------------------- Clear Session & Return Home --------------------
def reset_session():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.page = "home"
    st.rerun()

# -------------------- App Routing --------------------
def main():
    st.set_page_config(layout="wide", page_title="Image Metadata Reconciliation")

    # ---------- Initialize Required Session Keys ----------
    default_keys = {
        "page": "home",
        "aws_access_key": "",
        "aws_secret_key": "",
        "aws_session_token": "",
        "bucket_name": "",
        "file_key": "",
        "cust_aws_access_key": "",
        "cust_aws_secret_key": "",
        "cust_aws_session_token": "",
        "cust_bucket_name": "",
        "cust_file_key": "",
    }

    for key, default in default_keys.items():
        if key not in st.session_state:
            st.session_state[key] = default

    # ---------- Page Routing ----------
    if st.session_state.page == "home":
        home_page()
    elif st.session_state.page == "upload":
        upload_page()
    elif st.session_state.page == "results":
        results_page()

if __name__ == "__main__":
    main()
