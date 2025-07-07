import streamlit as st
import pandas as pd
import numpy as np
import re
import io
from datetime import datetime
import dateparser
from datetime import datetime
import re

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
    #master_data_es_file = st.file_uploader("Upload MasterDataES File", type=["csv", "xlsx"])

    #classifications_file = st.file_uploader("Upload Classifications File", type=["csv", "xlsx"])
    #attributes_file = st.file_uploader("Upload Attributes File", type=["csv", "xlsx"])

with col2:
    #master_data_es_file = st.file_uploader("Upload MasterDataES File", type=["csv", "xlsx"])
    sames_file = st.file_uploader("Upload SAMES File", type=["csv", "xlsx"])
    PO_file = st.file_uploader("Upload SAP DATA - PO NUMBER, DATE, REFERENCE", type=["csv", "xlsx"])

# Function to read either CSV or Excel files
def read_file(file):
    if file is not None:
        if file.name.endswith('csv'):
            try:
                # Try different encodings
                encodings = ['utf-8']#, 'latin1', 'cp1252', 'ISO-8859-1']
                for encoding in encodings:
                    try:
                        return pd.read_csv(file, encoding=encoding, dtype=str)
                    except UnicodeDecodeError:
                        continue
                st.error(f"Could not decode file {file.name} with any of the attempted encodings")
                return None
            except Exception as e:
                st.error(f"Error reading CSV file: {e}")
                return None
        elif file.name.endswith(('xlsx', 'xls')):
            try:
                # Read the first row to get column names
                temp_df = pd.read_excel(file, nrows=0)
                # Rewind the file pointer to the beginning
                file.seek(0)
                # Build dtype dict for all columns as str
                dtype_dict = {col: str for col in temp_df.columns}
                return pd.read_excel(file, dtype=dtype_dict)
            except Exception as e:
                st.error(f"Error reading Excel file: {e}")
                return None
    return None


def normalize_date_format(date_string):
    """
    Convert various date formats to dd/mm/yyyy format.
    Handles formats like: 07/09/2023_, 15-02-2023, 08/09/22, etc.
    """
    if not date_string:
        return None
    
    # Clean the date string - remove trailing underscores, spaces, and other unwanted characters
    cleaned_date = re.sub(r'[_\s]+', '', date_string.strip())
    cleaned_date = re.sub(r'[^\d\-/]', '', cleaned_date)  # Keep only digits, hyphens, and slashes
    
        # Return None if cleaned_date is empty or only whitespace
    if not cleaned_date or not cleaned_date.strip():
        return None
    
    # Try to parse with dateparser first (most reliable)
    try:
        parsed_date = dateparser.parse(cleaned_date, languages=['es', 'en'])
        if parsed_date:
            # Compare date (ignore time part)
            today = datetime.now().date()
            if parsed_date.date() > today:
                return None
            return parsed_date.strftime("%d/%m/%Y")
        
    except ImportError:
        pass
    except Exception:
        pass
    
    # Fallback to regex patterns for common formats
    date_patterns = [
        r'^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$',      # dd/mm/yyyy or dd-mm-yyyy
        r'^(\d{1,2})[/-](\d{1,2})[/-](\d{2})$',      # dd/mm/yy or dd-mm-yy
        r'^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$',      # yyyy/mm/dd or yyyy-mm-dd
    ]
    
    for pattern in date_patterns:
        try:
            match = re.match(pattern, cleaned_date)
            if match is None:
                continue
                
            # Get groups and verify we have exactly 3
            groups = match.groups()
            if groups is None or len(groups) != 3:
                continue
            
            # Now safely unpack - we know groups is not None and has 3 elements
            part1, part2, part3 = groups
            
            # Determine the format and assign day, month, year
            if len(part3) == 4:  # Full year format
                if len(part1) == 4:  # yyyy/mm/dd format
                    year, month, day = part1, part2, part3
                else:  # dd/mm/yyyy format  
                    day, month, year = part1, part2, part3
            else:  # 2-digit year format (dd/mm/yy)
                day, month, year_short = part1, part2, part3
                year_int = int(year_short)
                # Convert 2-digit year to 4-digit
                if year_int <= 30:
                    year = f"20{year_short}"
                else:
                    year = f"19{year_short}"
            
            # Convert to integers and validate
            day_int, month_int, year_int = int(day), int(month), int(year)
            
            # Basic range validation
            if not (1 <= month_int <= 12 and 1 <= day_int <= 31 and 1900 <= year_int <= 2100):
                continue
                
            # Create datetime object to validate the date (this will catch invalid dates like Feb 30)
            date_obj = datetime(year_int, month_int, day_int)
            return date_obj.strftime("%d/%m/%Y")
            
        except (ValueError, TypeError, AttributeError, OverflowError) as e:
            # ValueError: invalid date components or int conversion
            # TypeError: None unpacking or other type issues  
            # AttributeError: calling method on None
            # OverflowError: year out of range for datetime
            continue
        except Exception:
            # Catch any other unexpected errors
            continue
    
    return None
# Function to extract information from SAP notes
def extract_sap_notes_info(note):
    if pd.isna(note):
        return None, None, None
    
    # Convert to string if not already
    note = str(note)
    
    nhc_patterns = [
        r'NHC:?\s*\*\*\s*([^*]+)\s*\*\*',  # NHC: ** 12345 **
        r'NHC:?\s*\*\s*([^*]+)\s*\*',      # NHC: * 12345 *
        r'NHC:?\s+(\d+)',                  # NHC: 12345 or NHC  12345
        r'NHC:?\s*(?:NUMERO|NÚMERO|N[º°]|NUM)?\.?\s*:?\s*(\d+)',  # NHC: NUMERO: 12345, NHC Nº: 12345
        r'NHC:?\s*(?:NUM|NUMERO|NÚMERO|N[º°])?\s*\.?\s*(\w+)',    # NHC NUM. ABC123
        r'N\.?\s*H\.?\s*C\.?:?\s+(\d+)',   # N.H.C.: 12345 (with flexible spaces)
        r'NH:?\s+(\d+)',                   # NH: 12345 (with flexible spaces)
        r'HISTORIA:?\s*(?:NUM|NUMERO|NÚMERO|N[º°])?\s*\.?\s*(\d+)' # HISTORIA NUM. 12345
    ]
    
    nhc = None
    for pattern in nhc_patterns:
        nhc_match = re.search(pattern, note, re.IGNORECASE)
        if nhc_match:
            nhc = nhc_match.group(1).strip()
        # Count underscores in the matched string
            underscore_count = nhc.count('_')
        # If multiple underscores, set nhc to None
            if underscore_count > 1:
                nhc = None
            else:
            # Remove the underscore if there is exactly one
                nhc = nhc.replace('_', '')
            break
    else:
        nhc = None

    doctor_patterns = [
        r'N\.?\s*MEDICO:?\s*ºº\s*([^º]+)\s*ºº',  # N. MEDICO: ºº Dr. Smith ºº
        r'N\.?\s*MEDICO:?\s*\*\*\s*([^*]+)\s*\*\*',  # N. MEDICO: ** Dr. Smith **
        r'DOCTOR:?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)(?:\s+\w+:)',  # DOCTOR: Dr. Smith OTHER_FIELD:
        r'DR\.?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)(?:\s+\w+:)',  # DR. Dr. Smith OTHER_FIELD:
        r'DR\.?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)$',  # DR. Dr. Smith (at end of text)
        r'MEDICO:?\s*(?:\/|:)?\s*([A-Za-zÀ-ÿ\s.,]+?)(?:\s+\w+:)'  # MEDICO: Dr. Smith OTHER_FIELD:
    ]

    # Extract doctor name
    doctor = None
    for pattern in doctor_patterns:
        doctor_match = re.search(pattern, note, re.IGNORECASE)
        if doctor_match:
            doctor = doctor_match.group(1).strip()
            break

    # Step 1: Try to parse the entire note with dateparser first
    fecha_int = None
    try:
        parsed_date = dateparser.parse(note, languages=['es', 'en'])
        if parsed_date:
            fecha_int = parsed_date.strftime("%d/%m/%Y")
            fecha_int_norm = normalize_date_format(fecha_int)
            return nhc, fecha_int_norm, doctor
        
    except ImportError:
        print("dateparser not available, proceeding with pattern matching")
    except Exception as e:
        print(f"dateparser failed on full note: {e}")
    
    # Step 2: Extract specific date text using regex patterns
    fecha_patterns = [
        # F.INTERVENCIÓN: [[ 20.01.4.2025]], F.INTERVENCIÓN: [[ 20./01/2025]], F.INTERVENCIÓN: [[ 2.052025 ]], F.INTERVENCIÓN: [[ 2052025 ]]
        r'F\.?\s*INTERVENCI[ÓO]N:?\s*\[\[\s*([\d./\s]+)\s*\]\]',
        # FECHA INT.: [[ 03/05/2025 ]], FECHA INT [[ 03/05/2025 ]], FECHA INT: [[ 03/05/2025 ]]
        r'FECHA\s*INT\.?:?\s*\[\[\s*([\d./\s]+)\s*\]\]',
        r'FECHA\s*INT\.?\s*\[\[\s*([\d./\s]+)\s*\]\]',  # Handles missing colon
        # F.I. 26.03.2025, F.I 14/02/2025
        r'F\.?\s*I\.?\s*[:.]?\s*([\d]{1,2}[./][\d]{1,2}[./][\d]{2,4})',
        # FECHA: 19/04/23
        r'FECHA:?\s*([\d]{1,2}[./][\d]{1,2}[./][\d]{2,4})',
        # F.INTERVENCIÓN: 01/01/2023, F.INT: 01/01/2023
        r'F\.?\s*INTERVENCI[ÓO]N:?\s*([\d]{1,2}[./][\d]{1,2}[./][\d]{2,4})',
        r'F\.?\s*INT\.?:?\s*([\d]{1,2}[./][\d]{1,2}[./][\d]{2,4})',
        # FECHA DE LA INTERVENCIÓN: 01/01/2023
        r'FECHA\s*(?:DE)?\s*(?:LA)?\s*INTERVENCI[ÓO]N:?\s*([\d]{1,2}[./][\d]{1,2}[./][\d]{2,4})',
        # INTERVENIDO EL 01/01/2023
        r'INTERVENIDO:?\s*(?:EL|EN)?\s*([\d]{1,2}[./][\d]{1,2}[./][\d]{2,4})',
    ]
    
    fecha_raw = None
    for pattern in fecha_patterns:
        fecha_match = re.search(pattern, note, re.IGNORECASE)
        if fecha_match:
            fecha_raw = fecha_match.group(1).strip()
            break
    
    # If no pattern matched, return None for all values
    if not fecha_raw:
        return nhc, None, doctor
    
    # Step 2.1: Try dateparser on the extracted text
    try:
        parsed_date = dateparser.parse(fecha_raw, languages=['es', 'en'])
        if parsed_date:
            fecha_int = parsed_date.strftime("%d/%m/%Y")
            fecha_int_norm = normalize_date_format(fecha_int)
            return nhc, fecha_int_norm, doctor
        
    except ImportError:
        pass  # Already handled above
    except Exception as e:
        print(f"dateparser failed on extracted text '{fecha_raw}': {e}")
    
    # Step 3: Fallback to regex parsing if dateparser fails
    # Common date patterns: dd/mm/yyyy, dd-mm-yyyy, d/m/yy, etc.
    date_regex_patterns = [
        r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',      # dd/mm/yyyy or dd-mm-yyyy
        r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',      # dd/mm/yy or dd-mm-yy
        r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',      # yyyy/mm/dd or yyyy-mm-dd
    ]
    
    for regex_pattern in date_regex_patterns:
        date_match = re.search(regex_pattern, fecha_raw)
        if date_match:
            try:
                # Get groups and check if we have exactly 3
                groups = date_match.groups()
                if not groups or len(groups) != 3:
                    continue
                
                # Safe unpacking
                part1, part2, part3 = groups
                
                # Handle different date formats
                if len(part3) == 4:  # Full year
                    if len(part1) == 4:  # yyyy/mm/dd format
                        year, month, day = part1, part2, part3
                    else:  # dd/mm/yyyy format
                        day, month, year = part1, part2, part3
                else:  # 2-digit year
                    day, month, year_short = part1, part2, part3
                    # Convert 2-digit year to 4-digit (assuming 20xx for years 00-30, 19xx for 31-99)
                    year_int = int(year_short)
                    if year_int <= 30:
                        year = f"20{year_short}"
                    else:
                        year = f"19{year_short}"
                
                # Validate and create datetime object
                day, month, year = int(day), int(month), int(year)
                
                # Basic validation
                if 1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100:
                    # Create datetime object to validate the date
                    date_obj = datetime(year, month, day)
                    fecha_int = date_obj.strftime("%d/%m/%Y")
                    fecha_int_norm = normalize_date_format(fecha_int)
                    return nhc, fecha_int_norm, doctor
                    
            except (ValueError, IndexError, TypeError) as e:
                print(f"Date validation failed for '{fecha_raw}': {e}")
                continue
    
    # If we reach here, date parsing failed
    # But we still want to return the nhc and doctor if found
    if fecha_int:
        fecha_int_norm = normalize_date_format(fecha_int)
        st.success(f"Normalized date: {fecha_int_norm}")
        st.success(f"Results: NHC={nhc}, Date={fecha_int}, Doctor={doctor}")
    
    return nhc, None, doctor

# Process files when all are uploaded
if st.button("Process Files", disabled=not all([base_file, sap_notes_file])):#,  master_data_es_file])):
    with st.spinner("Processing files..."):
        # Read all files
        base_df = read_file(base_file)
        sap_notes_df = read_file(sap_notes_file)
        #classifications_df = read_file(classifications_file)
        #master_data_es_df = read_file(master_data_es_file)
        sames_df = read_file(sames_file) if sames_file else None
        po_df = read_file(PO_file)
        #attributes_df = read_file( attributes_file)
        
        if all([base_df is not None, sap_notes_df is not None,  #master_data_es_df is not None, 
                po_df is not None]):
            # Display original dataframes
            st.subheader("Original Data Preview")
            tabs = st.tabs(["Base", "SAP Notes", # "MasterDataES", 
                            "SAMES", "PO"])
            
            with tabs[0]:
                st.write("Base File Preview:")
                st.dataframe(base_df.head())
                
            with tabs[1]:
                st.write("SAP Notes File Preview:")
                st.dataframe(sap_notes_df.head())
                
            # with tabs[2]:
            #     st.write("Classifications File Preview:")
            #     st.dataframe(classifications_df.head())
                
            # with tabs[2]:
            #     st.write("MasterDataES File Preview:")
            #     st.dataframe(master_data_es_df.head())
                
            with tabs[2]:
                if sames_df is not None:
                    st.write("SAMES File Preview:")
                    st.dataframe(sames_df.head())
                else:
                    st.write("SAMES File not uploaded")
            
            with tabs[3]:
                st.write("SAP Data - File Preview:")
                st.dataframe(po_df.head())
            
            # with tabs[6]:
            #     st.write("SAP Data - File Preview:")
            #     st.dataframe(attributes_df.head())

            # 1. Match with Classifications
            st.subheader("Step 2: Matching Data")

            st.write("Matching with SAP file...")

            base_df['doc_nr_formatted'] = base_df['IDOrder'].astype(str).str.zfill(10)
            po_df['doc_nr_formatted'] = po_df['SD Document'].astype(str).str.zfill(10)

            # Create a mapping dictionary from po_df
            po_mapping = dict(zip(po_df['doc_nr_formatted'], po_df['Purchase order number']))
            po_reference_mapping = dict(zip(po_df['doc_nr_formatted'], po_df['Your Reference']))

            # Fill the 'Purchase order number' column in df using the mapping
            base_df['SO PO Number'] = None
            base_df['Your Reference'] = None
            base_df['SO PO Number'] = base_df['doc_nr_formatted'].map(po_mapping)
            base_df['Your Reference'] = base_df['doc_nr_formatted'].map(po_reference_mapping)

            # Clean up - remove the temporary formatted column if you don't need it
            base_df.drop('doc_nr_formatted', axis=1, inplace=True)
            po_df.drop('doc_nr_formatted', axis=1, inplace=True)

            # Optional: Check for any unmatched records
            unmatched = base_df['SO PO Number'].isna().sum()
            if unmatched > 0:
                print(f"Warning: {unmatched} records could not be matched")
            
            st.success(f"SAP data mapping completed: {sum(base_df['SO PO Number'].notna())} rows updated")

            # st.write("Matching with Classifications file...")
            
            #  # Check if the required columns exist in both dataframes
            # if "ISIS Product Hierarchy Level 2 Desc" in base_df.columns and classifications_df is not None:
            #     # Find the appropriate columns in Classifications file
            #     hierarchy_col = None
            #     classification_col = None
                
            #     # Look for column names containing these patterns
            #     for col in classifications_df.columns:
            #         if "ISIS Product Hierarchy Level 2" in col:
            #             hierarchy_col = col
            #         elif "Classificación Comisiones" in col:
            #             classification_col = col
            
                
            #     # Display the columns we're using
            #     if hierarchy_col and classification_col:
            #         st.info(f"Using columns: '{hierarchy_col}' to match with 'ISIS Product Hierarchy Level 2 Desc' and '{classification_col}' for classification values")
                    
            #         # Create a mapping dictionary from classifications_df
            #         classifications_dict = dict(zip(
            #             classifications_df[hierarchy_col], 
            #             classifications_df[classification_col]
            #         ))
                    
            #         # Apply mapping to base_df
            #         base_df["Clasificación Comisiones"] = base_df["ISIS Product Hierarchy Level 2 Desc"].map(classifications_dict)
                    
            #         # Display the mapping results
            #         st.success(f"Classification mapping completed: {sum(base_df['Clasificación Comisiones'].notna())} rows updated")
            #     else:
            #         st.error("Could not find appropriate columns in Classifications file")
            # else:
            #     st.warning("Could not match classifications - column 'ISIS Product Hierarchy Level 2 Desc' not found in Base file or Classifications file is empty")
            
            # 2. Join with MasterDataES
            # st.write("Joining with MasterDataES file...")
            # if all(col in base_df.columns for col in ["IDBillDoc", "IDBillDocItem"]) and master_data_es_df is not None:
            #     # Find the corresponding columns in MasterDataES
            #     bill_doc_col = next((col for col in master_data_es_df.columns if "billdoc" in col.lower() and "item" not in col.lower()), None)
            #     bill_doc_item_col = next((col for col in master_data_es_df.columns if ("billdocitem" in col.lower()) or ("billdoc" in col.lower() and "item" in col.lower())), None)
            #     current_corrected_id_col = next((col for col in master_data_es_df.columns if "currentcorrected" in col.lower() and ("id" in col.lower() or col.lower().endswith("id"))), None)
                
            #     # Check if all required columns were found
            #     if all([bill_doc_col, bill_doc_item_col, current_corrected_id_col]):
            #         st.info(f"Joining on '{bill_doc_col}', '{bill_doc_item_col}' to get '{current_corrected_id_col}'")
                    
            #         # Perform the join directly
            #         result_df = pd.merge(
            #             base_df,
            #             master_data_es_df[[bill_doc_col, bill_doc_item_col, current_corrected_id_col]],
            #             on = ["IDBillDoc", "IDBillDocItem"],
            #             # left_on=["IDBillDoc", "IDBillDocItem"],
            #             # right_on=[bill_doc_col, bill_doc_item_col],
            #             how="left"
            #         )
                    
            #         # Rename the columns to standardized names
            #         result_df = result_df.rename(columns={
            #             'IDCurrentCorrected_y': "IDCurrentCorrected"
            #         })
                    
                    # # Drop the redundant columns from the join
                    # result_df = result_df.drop(columns=[bill_doc_col, bill_doc_item_col])
                    
                    # Update the base dataframe
                    #base_df = result_df
                    
                    # Display the results
                    #st.success(f"MasterDataES joining completed: {base_df.columns}")

            #         st.success(f"MasterDataES joining completed: {sum(base_df['IDCurrentCorrected'].notna())} rows updated")
            #     else:
            #         missing_cols = []
            #         if not bill_doc_col: missing_cols.append("BillDoc")
            #         if not bill_doc_item_col: missing_cols.append("BillDocItem")
            #         if not current_corrected_id_col: missing_cols.append("CurrentCorrectedID")
            #         st.error(f"Could not find required columns in MasterDataES: {', '.join(missing_cols)}")
            # else:
            #     st.warning("Could not join with MasterDataES - required columns not found in Base file")
            
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
                    base_df["NHC"] = None
                    base_df["F. Int - Textos"] = None
                    base_df["DOCTOR"] = None

                    NHC_From_SO_PO_patterns = r'NHC\s*(?:CIC\s+(\d+(?:\s*/\s*\d+)?)|:?\s*\*{0,2}\s*([A-Za-z0-9]+(?:\s*/\s*[A-Za-z0-9]+)?)\s*\*{0,2})'

                    for idx, note in enumerate(base_df["SAPNotes"]):
                        if pd.notna(note):
                            nhc, fecha_int, doctor = extract_sap_notes_info(note)
                            if nhc is not None:
                                base_df.at[idx, "NHC"] = nhc
                            else:
                                so_po = base_df.at[idx, "SO PO Number"]
                                match = re.search(NHC_From_SO_PO_patterns, so_po)

                                if match:
                                    nhc = match.group(1) or match.group(2)  # Extract the captured group (the number or numbers with slash)
                                    base_df.at[idx, "NHC"] = nhc
                                else:
                                    base_df.at[idx, "NHC"] = 'NHC NO INFORMADO'

                            if fecha_int is not None and fecha_int != "None":
                                base_df.at[idx, "F. Int - Textos"] = fecha_int
                            else:
                                try:
                                    invoice_date_value = base_df.at[idx, "Invoice Date"]
                                except:
                                    invoice_date_value = base_df.at[idx, "Date"]

                                #     # Convert to datetime if not already, handle errors gracefully
                                # if not isinstance(invoice_date_value, pd.Timestamp):
                                invoice_date_value = pd.to_datetime(invoice_date_value, errors='coerce')
                                if pd.notnull(invoice_date_value):
                                    base_df.at[idx, "F. Int - Textos"] = invoice_date_value.strftime("%d/%m/%Y")

                            if doctor and doctor.strip() and not re.fullmatch(r"_+", doctor.strip()):
                                base_df.at[idx, "DOCTOR"] = doctor.strip()
                            else:
                                base_df.at[idx, "DOCTOR"] = 'NO INFORMADO'
                        else:
                            invoice_date_value = base_df.at[idx, "Invoice Date"]
                             # Convert to datetime if not already, handle errors gracefully
                            if not isinstance(invoice_date_value, pd.Timestamp):
                                invoice_date_value = pd.to_datetime(invoice_date_value, errors='coerce')

                                if pd.notnull(invoice_date_value):
                                    base_df.at[idx, "F. Int - Textos"] = invoice_date_value.strftime("%d/%m/%Y")
                            
                            so_po = base_df.at[idx, "SO PO Number"]
                            match = re.search(NHC_From_SO_PO_patterns, so_po)

                            if match:
                                nhc = match.group(1) or match.group(2) # Extract the captured group (the number or numbers with slash)
                                base_df.at[idx, "NHC"] = nhc
                            else:
                                base_df.at[idx, "NHC"] = 'NHC NO INFORMADO'
                    
                    def clean_doctor(value):
                     # Handle NaN or empty strings
                        if pd.isna(value) or str(value).strip() == '':
                            return 'NO INFORMADO'
                        # Handle hyphen
                        if str(value).strip() == '-':
                            return 'No informado'
                        # Handle numbers
                        if str(value).strip().isdigit():
                            return 'NO INFORMADO'
                        return value

                    base_df['DOCTOR'] = base_df['DOCTOR'].apply(clean_doctor)

                    st.success("SAP Notes extraction completed")
                else:
                    st.warning("Could not find required columns in SAP Notes file")
            else:
                st.warning("Could not process SAP Notes - 'IDOrder' column not found in Base file")
            
            # Remove temporary join key
            if "JoinKey" in base_df.columns:
                base_df = base_df.drop(columns=["JoinKey"])

            # st.write("Matching with Attributes file...")

            # # Create a mapping dictionary from df_attribute
            # # Convert Attribute 2 to string and strip whitespace for better matching
            # attr_mapping = dict(zip(
            #     attributes_df['Attribute 2'].astype(str).str.strip(), 
            #     attributes_df['Description']
            # ))

            # # Function to replace two-digit patterns with descriptions
            # def replace_two_digit_codes(text):
            #     if pd.isna(text):
            #         return text
                
            #     text = str(text)
                
            #     # Find all two-digit patterns (like '01', '02', etc.)
            #     # This regex looks for exactly 2 digits
            #     pattern = r'\b\d{2}\b'
                
            #     def replace_match(match):
            #         code = match.group()
            #         # Look up the code in our mapping
            #         if code in attr_mapping:
            #             return str(attr_mapping[code])
            #         else:
            #             return code  # Return original if no match found
                
            #     # Replace all matches in the text
            #     result = re.sub(pattern, replace_match, text)
            #     return result

            # # Apply the replacement to the Payer_Attr2 column
            # base_df['Payer_Attr2'] = base_df['Payer_Attr2'].apply(replace_two_digit_codes)
            # base_df['SoldTo_Attr2'] = base_df['SoldTo_Attr2'].apply(replace_two_digit_codes)

            # # Optional: Show some statistics about the replacement
            # print("Replacement completed!")
            # print(f"Total records processed: {len(base_df)}")

            # # Check for any two-digit codes that might not have been matched
            # remaining_codes = base_df['Payer_Attr2'].astype(str).str.extractall(r'(\b\d{2}\b)')[0].unique()
            # unmatched_codes = [code for code in remaining_codes if code not in attr_mapping.keys()]
            # if unmatched_codes:
            #     print(f"Warning: These two-digit codes were not found in df_attribute: {unmatched_codes}")
            
            # st.success("Number Attributes replacement completed")

            # 3. Extract data from SAP Notes
            st.write("Extracting data from SAMES..")

            # # Create a mapping dictionary from po_df
            sames_mapping = dict(zip(sames_df['Nº Historial Clínico'], sames_df['Comisionista (11)']))

            base_df['INICIADOR SAMES'] = None
            base_df['INICIADOR SAMES'] = base_df["NHC"].map(sames_mapping)


            # Optional: Check for any unmatched records
            unmatched = base_df['INICIADOR SAMES'].isna().sum()
            if unmatched > 0:
                print(f"Warning: {unmatched} records could not be matched")
            sum_not_matched = sum(base_df['INICIADOR SAMES'].notna())
            base_df['DOCTOR'] = base_df['DOCTOR'].fillna('NO INFORMADO')

            st.success(f"SAMES mapping completed: {sum_not_matched} rows matched")


            # Show the processed dataframe
            st.subheader("Step 3: Results")
            st.write("Processed Base File Preview:")
            
            #base_df["F. Int - Textos"] = base_df["F. Int - Textos"].astype(str)
            base_df["Invoice Date"] = pd.to_datetime(base_df["Invoice Date"], errors='coerce').dt.strftime("%d/%m/%Y")
            base_df.drop(columns=['F. Int - Formula','NHC - Textos','NHC - Formula','Dr - Textos'], inplace=True)
            base_df['INICIADOR SAMES'] = base_df['INICIADOR SAMES'].fillna('NHC NO ENCONTRADO')
            cols = [col for col in base_df.columns if col != 'SAPNotes'] + ['SAPNotes']
            df = base_df[cols]
            st.dataframe(base_df.head(100))
            
            # Download the processed file
            st.subheader("Step 4: Download")
            csv = base_df.to_csv(index=False, encoding='utf-8')
            st.download_button(
                label="Download Processed Base File",
                data=csv,
                file_name="processed_base_file.csv",
                mime="text/csv"
            )
        else:
            st.error("Please upload all required files (Base, SAP Notes, MasterDataES)")

st.markdown("---")
st.write("This app processes your data files and performs lookups and matching operations to consolidate data into the Base file.")