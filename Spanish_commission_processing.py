import streamlit as st
import pandas as pd
import numpy as np
import re
import io
from datetime import datetime
import dateparser


st.set_page_config(
    page_title="Spanish Commissions Data Processing Tool",
    page_icon="📋",
    layout="wide"
)

st.title("Spanish Commissions Data Processing Tool")
st.write("Upload your files to match and consolidate data from multiple sources into the Base file.")

# File upload section
st.subheader("Step 1: Upload Files")
col1, col2 = st.columns(2)

with col1:
    base_file = st.file_uploader("Upload Base File", type=["csv", "xlsx"])
    sap_notes_file = st.file_uploader("Upload SAP Notes File", type=["csv", "xlsx"])
    classifications_file = st.file_uploader("Upload Classifications File", type=["csv", "xlsx"])

with col2:
    master_data_es_file = st.file_uploader("Upload MasterDataES File", type=["csv", "xlsx"])
    sames_file = st.file_uploader("Upload SAMES File", type=["csv", "xlsx"])

# Function to read either CSV or Excel files
def read_file(file):
    if file is not None:
        if file.name.endswith('csv'):
            try:
                # Try different encodings
                encodings = ['utf-8', 'latin1', 'cp1252', 'ISO-8859-1']
                for encoding in encodings:
                    try:
                        return pd.read_csv(file, encoding=encoding)
                    except UnicodeDecodeError:
                        continue
                st.error(f"Could not decode file {file.name} with any of the attempted encodings")
                return None
            except Exception as e:
                st.error(f"Error reading CSV file: {e}")
                return None
        elif file.name.endswith(('xlsx', 'xls')):
            try:
                return pd.read_excel(file)
            except Exception as e:
                st.error(f"Error reading Excel file: {e}")
                return None
    return None

# Function to extract information from SAP notes
def extract_sap_notes_info(note):
    if pd.isna(note):
        return None, None, None
    
    # Convert to string if not already
    note = str(note)
    
    nhc_patterns = [
    r'NHC:?\s*\*\*\s*([^*]+)\s*\*\*',        # NHC: ** 12345 **
    r'NHC:?\s*\*\s*([^*]+)\s*\*',            # NHC: * 12345 *
    r'NHC:?\s*(\d+)',                        # NHC: 12345 or NHC 12345
    r'NHC:?\s*(?:NUMERO|NÚMERO|N[º°]|NUM)?\.?\s*:?\s*(\d+)',  # NHC: NUMERO: 12345, NHC Nº: 12345
    r'NHC:?\s*(?:NUM|NUMERO|NÚMERO|N[º°])?\s*\.?\s*(\w+)',    # NHC NUM. ABC123
    r'N\.?\s*H\.?\s*C\.?:?\s*(\d+)',         # N.H.C.: 12345
    r'NH:?\s*(\d+)',                         # NH: 12345
    r'HISTORIA:?\s*(?:NUM|NUMERO|NÚMERO|N[º°])?\s*\.?\s*(\d+)' # HISTORIA NUM. 12345
]
    nhc = None
    for pattern in nhc_patterns:
        nhc_match = re.search(pattern, note, re.IGNORECASE)
        if nhc_match:
            nhc = nhc_match.group(1).strip()
            break

    fecha_patterns = [
    r'F\.?\s*INTERVENCIÓN:?\s*\[\[\s*([^\]]+)',       # F.INTERVENCIÓN: [[ date
    r'F\.?\s*INT\.?:?\s*\[\[\s*([^\]]+)',             # F.INT: [[ date
    r'F\.?\s*INTERVENCIÓN:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',  # F.INTERVENCIÓN: 01/01/2023
    r'F\.?\s*INT\.?:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',        # F.INT: 01/01/2023
    r'FECHA\s*(?:DE)?\s*(?:LA)?\s*INTERVENCIÓN:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',  # FECHA DE LA INTERVENCIÓN: 01/01/2023
    r'INTERVENIDO:?\s*(?:EL|EN)?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'                  # INTERVENIDO EL 01/01/2023
]
    
    # Extract intervention date with various formats
    fecha_raw = None
    for pattern in fecha_patterns:
        fecha_match = re.search(pattern, note, re.IGNORECASE)
        if fecha_match:
            fecha_raw = fecha_match.group(1).strip()
            break
    fecha_raw = note

    # Try to parse the date using dateparser if available
    fecha_int = None
    if fecha_raw:
        try:
            date_matches = dateparser.parse(fecha_raw, languages=['es', 'en'])
            if date_matches :
                fecha_int = date_matches
            else:
                fecha_int_match = re.search(r'F\.INTERVENCIÓN:\s*\[\[\s*([^\]]+)', note)
                fecha_raw = fecha_int_match.group(1).strip() if fecha_int_match else None
                fecha_int = fecha_raw  # Keep the original text if parsing fails
        except ImportError:
            # If dateparser is not installed, return the raw text
            fecha_int = fecha_raw
            st.warning("For better date parsing, install dateparser: pip install dateparser")
    
    doctor_patterns = [
    r'N\.?\s*MEDICO:?\s*ºº\s*([^º]+)\s*ºº',                  # N. MEDICO: ºº Dr. Smith ºº
    r'N\.?\s*MEDICO:?\s*\*\*\s*([^*]+)\s*\*\*',              # N. MEDICO: ** Dr. Smith **
    r'DOCTOR:?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)(?:\s+\w+:)', # DOCTOR: Dr. Smith OTHER_FIELD:
    r'DR\.?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)(?:\s+\w+:)',    # DR. Dr. Smith OTHER_FIELD:
    r'DR\.?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)$',              # DR. Dr. Smith (at end of text)
    r'MEDICO:?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)(?:\s+\w+:)'  # MEDICO: Dr. Smith OTHER_FIELD:
]
    # Extract doctor name
    doctor = None
    for pattern in doctor_patterns:
        doctor_match = re.search(pattern, note, re.IGNORECASE)
        if doctor_match:
            doctor = doctor_match.group(1).strip()
            break
    # doctor_match = re.search(r'N\.\s*MEDICO:\s*ºº\s*([^º]+)\s*ºº', note)
    # doctor = doctor_match.group(1).strip() if doctor_match else None
    
    return nhc, fecha_int, doctor

# Process files when all are uploaded
if st.button("Process Files", disabled=not all([base_file, sap_notes_file, classifications_file, master_data_es_file])):
    with st.spinner("Processing files..."):
        # Read all files
        base_df = read_file(base_file)
        sap_notes_df = read_file(sap_notes_file)
        classifications_df = read_file(classifications_file)
        master_data_es_df = read_file(master_data_es_file)
        sames_df = read_file(sames_file) if sames_file else None
        
        if all([base_df is not None, sap_notes_df is not None, classifications_df is not None, master_data_es_df is not None]):
            # Display original dataframes
            st.subheader("Original Data Preview")
            tabs = st.tabs(["Base", "SAP Notes", "Classifications", "MasterDataES", "SAMES"])
            
            with tabs[0]:
                st.write("Base File Preview:")
                st.dataframe(base_df.head())
                
            with tabs[1]:
                st.write("SAP Notes File Preview:")
                st.dataframe(sap_notes_df.head())
                
            with tabs[2]:
                st.write("Classifications File Preview:")
                st.dataframe(classifications_df.head())
                
            with tabs[3]:
                st.write("MasterDataES File Preview:")
                st.dataframe(master_data_es_df.head())
                
            with tabs[4]:
                if sames_df is not None:
                    st.write("SAMES File Preview:")
                    st.dataframe(sames_df.head())
                else:
                    st.write("SAMES File not uploaded")
            
            # 1. Match with Classifications
            st.subheader("Step 2: Matching Data")
            st.write("Matching with Classifications file...")
            
             # Check if the required columns exist in both dataframes
            if "ISIS Product Hierarchy Level 2 Desc" in base_df.columns and classifications_df is not None:
                # Find the appropriate columns in Classifications file
                hierarchy_col = None
                classification_col = None
                
                # Look for column names containing these patterns
                for col in classifications_df.columns:
                    if "ISIS Product Hierarchy Level 2" in col:
                        hierarchy_col = col
                    elif "Classificación Comisiones" in col:
                        classification_col = col
                
                # # If exact matches not found, use more flexible matching
                # if hierarchy_col is None:
                #     for col in classifications_df.columns:
                #         if "ISIS" in col and "Level 2" in col:
                #             hierarchy_col = col
                #             break
                
                # if classification_col is None:
                #     for col in classifications_df.columns:
                #         if "Clasific" in col and "Comision" in col:
                #             classification_col = col
                #             break
                
                # # If still not found, use the first and second columns
                # if hierarchy_col is None and len(classifications_df.columns) > 0:
                #     hierarchy_col = classifications_df.columns[0]
                    
                # if classification_col is None and len(classifications_df.columns) > 1:
                #     classification_col = classifications_df.columns[1]
                
                # Display the columns we're using
                if hierarchy_col and classification_col:
                    st.info(f"Using columns: '{hierarchy_col}' to match with 'ISIS Product Hierarchy Level 2 Desc' and '{classification_col}' for classification values")
                    
                    # Create a mapping dictionary from classifications_df
                    classifications_dict = dict(zip(
                        classifications_df[hierarchy_col], 
                        classifications_df[classification_col]
                    ))
                    
                    # Apply mapping to base_df
                    base_df["Clasificación Comisiones"] = base_df["ISIS Product Hierarchy Level 2 Desc"].map(classifications_dict)
                    
                    # Display the mapping results
                    st.success(f"Classification mapping completed: {sum(base_df['Clasificación Comisiones'].notna())} rows updated")
                else:
                    st.error("Could not find appropriate columns in Classifications file")
            else:
                st.warning("Could not match classifications - column 'ISIS Product Hierarchy Level 2 Desc' not found in Base file or Classifications file is empty")
            
            # 2. Join with MasterDataES
            st.write("Joining with MasterDataES file...")
            if all(col in base_df.columns for col in ["IDBillDoc", "IDBillDocItem"]) and master_data_es_df is not None:
                # Find the corresponding columns in MasterDataES
                bill_doc_col = next((col for col in master_data_es_df.columns if "billdoc" in col.lower() and "item" not in col.lower()), None)
                bill_doc_item_col = next((col for col in master_data_es_df.columns if ("billdocitem" in col.lower()) or ("billdoc" in col.lower() and "item" in col.lower())), None)
                current_corrected_id_col = next((col for col in master_data_es_df.columns if "currentcorrected" in col.lower() and ("id" in col.lower() or col.lower().endswith("id"))), None)
                
                # Check if all required columns were found
                if all([bill_doc_col, bill_doc_item_col, current_corrected_id_col]):
                    st.info(f"Joining on '{bill_doc_col}', '{bill_doc_item_col}' to get '{current_corrected_id_col}'")
                    
                    # Perform the join directly
                    result_df = pd.merge(
                        base_df,
                        master_data_es_df[[bill_doc_col, bill_doc_item_col, current_corrected_id_col]],
                        on = ["IDBillDoc", "IDBillDocItem"],
                        # left_on=["IDBillDoc", "IDBillDocItem"],
                        # right_on=[bill_doc_col, bill_doc_item_col],
                        how="left"
                    )
                    
                    # Rename the columns to standardized names
                    result_df = result_df.rename(columns={
                        'IDCurrentCorrected_y': "IDCurrentCorrected"
                    })
                    
                    # # Drop the redundant columns from the join
                    # result_df = result_df.drop(columns=[bill_doc_col, bill_doc_item_col])
                    
                    # Update the base dataframe
                    base_df = result_df
                    
                    # Display the results
                    st.success(f"MasterDataES joining completed: {base_df.columns}")

                    st.success(f"MasterDataES joining completed: {sum(base_df['IDCurrentCorrected'].notna())} rows updated")
                else:
                    missing_cols = []
                    if not bill_doc_col: missing_cols.append("BillDoc")
                    if not bill_doc_item_col: missing_cols.append("BillDocItem")
                    if not current_corrected_id_col: missing_cols.append("CurrentCorrectedID")
                    st.error(f"Could not find required columns in MasterDataES: {', '.join(missing_cols)}")
            else:
                st.warning("Could not join with MasterDataES - required columns not found in Base file")
            
            # 3. Extract data from SAP Notes
            st.write("Extracting data from SAP Notes...")
            
            if "IDOrder" in base_df.columns and sap_notes_df is not None:
                # Find the right columns in SAP Notes file
                order_col = [col for col in sap_notes_df.columns if 'order' in col.lower()][0] if any('order' in col.lower() for col in sap_notes_df.columns) else None
                notes_col = [col for col in sap_notes_df.columns if 'note' in col.lower() or 'text' in col.lower()][0] if any(col in col.lower() for col in sap_notes_df.columns for col in ['note', 'text']) else None
                
                if order_col and notes_col:
                    # Create a mapping from IDOrder to SAP Notes
                    notes_mapping = dict(zip(sap_notes_df[order_col], sap_notes_df[notes_col]))
                    
                    # Apply mapping to get SAP Notes
                    base_df["SAPNotes"] = base_df["IDOrder"].map(notes_mapping)
                    
                    # Extract information from SAP Notes
                    # Extract information from SAP Notes
                    base_df["NHC - Textos"] = None
                    base_df["F. Int - Textos"] = None
                    base_df["Surgeon Name"] = None

                    for idx, note in enumerate(base_df["SAPNotes"]):
                        if pd.notna(note):
                            nhc, fecha_int, doctor = extract_sap_notes_info(note)
                            base_df.at[idx, "NHC - Textos"] = nhc
                            base_df.at[idx, "F. Int - Textos"] = fecha_int
                            base_df.at[idx, "Surgeon Name"] = doctor
                    
                    st.success("SAP Notes extraction completed")
                else:
                    st.warning("Could not find required columns in SAP Notes file")
            else:
                st.warning("Could not process SAP Notes - 'IDOrder' column not found in Base file")
            
            # Remove temporary join key
            if "JoinKey" in base_df.columns:
                base_df = base_df.drop(columns=["JoinKey"])
            
            # Show the processed dataframe
            st.subheader("Step 3: Results")
            st.write("Processed Base File Preview:")
            st.dataframe(base_df.head(100))
            
            # Download the processed file
            st.subheader("Step 4: Download")
            csv = base_df.to_csv(index=False)
            st.download_button(
                label="Download Processed Base File",
                data=csv,
                file_name="processed_base_file.csv",
                mime="text/csv"
            )
        else:
            st.error("Please upload all required files (Base, SAP Notes, Classifications, MasterDataES)")

st.markdown("---")
st.write("This app processes your data files and performs lookups and matching operations to consolidate data into the Base file.")