import os
from pathlib import Path
import re
import sys
from typing import List, Tuple, Dict, Any, cast

import pandas as pd
import numpy as np
from docx import Document

from pdfTableReader import PDFTableReader
from .tableDocHandler import TableDocxHandler
from .docxTemplator import DocxTemplator
import utils

def fetch_inputs(config):

    prompts = config.get('input_prompts', {})
    inputs = {}
    for k, v in prompts.items():
        inp  = input(v)
        if k == 'state':
            inp = inp.upper()
        inputs[k] = inp
    inputs['village_abbr'] = inputs['village'][:3].upper()

    return inputs

def initialize_output_dir_structure(config, inputs):
    # Create main directory
    main_dir = Path(config['main_dirname_format'].format(**inputs))
    utils.create_dir(main_dir)

    # Create output subdirs
    subdirs = config.get('OUTPUT_SUBDIRS', [])
    for s in subdirs:
        utils.create_dir(main_dir / s)
        print(f'Created Folder {s}')

    return main_dir

def get_dgps_and_reports_dir(config, inputs, main_dir):

    # Get input dgps dir
    dgps_dir = utils.find_dir(inputs['inputs_dir'], config['dgps_inputs'])

    # Move files to reports dir
    reports_dirname = config.get("OUTPUT_SUBDIRS", [])[7]
    reports_dir = main_dir / reports_dirname

    return dgps_dir, reports_dir

def move_files_to_reports_dir(config, dgps_dir, baseline_dir, reports_dir):

    report_names = config['report_patterns']
    renamed = config['renamed_reports']

    for f in baseline_dir.iterdir():
        if any(rep in f.name.lower() for rep in report_names['baseline']):
            utils.copy_file(
                f, reports_dir,
                renamed=renamed['baseline'].format(filename=f.name)
            )
        elif any(rep in f.name.lower() for rep in report_names['network']):
            utils.copy_file(
                f, reports_dir,
                renamed=renamed['network'].format(filename=f)
            )
        elif any(rep in f.name.lower() for rep in report_names['other_reports']):
            utils.copy_file(f, reports_dir)

    # Logsheets
    logsheets_dir = utils.find_dir(
        dgps_dir, config['dir_patterns']['log_sheets']
    )
    for f in logsheets_dir.iterdir():
        if (
            inputs['village_abbr'].lower() in f.name.lower() and
            f.name.lower().endswith('sheet.docx')
        ):
            filename = renamed['gnss_log_sheet']
            utils.copy_file(f, reports_dir, renamed=filename)

    #rtk checkpoints
    rtk_ckpt_dir = utils.find_dir(
        dgps_dir, config['dir_patterns']['rtk_checkpoint']
    )
    # Look for a particular report
    rtk_found = False
    for f in rtk_ckpt_dir.iterdir():
        if (
            inputs['village_abbr'].lower() in f.name.lower() and
            'report' in f.name.lower()
        ):
            filename = renamed['nrtk_checkpoint_report']
            utils.pdf_to_docx(f, reports_dir, filename)
            rtk_found = True
            break

    if not rtk_found:
        raise FileNotFoundError()

def save_coords_file_get_pids(
    config: Dict[str, Any], reports_dir, date_dirpath, sdir, date
):

    network_report = None
    for rep in reports_dir.iterdir():
        if 'network' in rep.name.lower() and date in rep.name.lower():
            network_report = rep
            break
    if not network_report:
        return

    table_reader = PDFTableReader(network_report)
    cors_cols = cast(Dict[str, str], config['cors_columns'])

    # get tables from pdf and save to excel spreadsheet
    tables = table_reader.extract_tables()
    merged = table_reader.merge_tables(tables, key_col='point_id')
    merged = table_reader.remove_nums_in_col(merged, col='point_id')
    final_table = merged[list(cors_cols.keys())]
    final_table = final_table.rename(columns=cors_cols) # type: ignore

    # save this dataframe as an excel spreadsheet in the date_dirpath
    excel_filename = config['filename_templates']['cors_excel']
    date_dirpath = Path(date_dirpath)

    excel_path = date_dirpath / excel_filename.format(date=sdir)
    final_table.to_excel(excel_path, index=False)
    print(f'Saved cors excel to {excel_path}')

    return table_reader.parse_col_vals(colname='point id')

def delete_older_files(config, date_dirpath, pids, reports_dir, date):
    date_dirpath = Path(date_dirpath)
    seen = {}
    dups = []
    for f in date_dirpath.iterdir():
         # check if character part of the file is in pids
        match = re.match(r'[a-zA-Z]+', f.name)
        if match:
            char_part = match.group(0)
            if char_part not in seen:
                seen[char_part] = 1
            else:
                dups.append(f)
        else:
            char_part = ""
        if utils.is_match_with_pids(char_part, pids):
            continue
        else:
            f.unlink()
    # using baseline report to get latest files
    baseline_report = None
    for rep in reports_dir.iterdir():
        if 'base' in rep.name.lower() and date in rep.name.lower():
            baseline_report = rep
            break
    if not baseline_report:
        return
    table_reader = PDFTableReader(baseline_report)
    # dictionary that tells you whether to save the file
    criteria = {}
    latest_files = table_reader.extract_latest_filenames(
        config['pdf_keywords']['data_file_kws']
    )
    if not dups:
        return
    else:
        for file in latest_files:
            match = re.match(r'([a-zA-Z]+)(\d+)', file)
            if match:
                c, n = match.groups()
                if c not in criteria:
                    criteria[c] = []
                criteria[c].append(n)

        for d in date_dirpath.iterdir():
            # match character part
            match = re.match(r'([a-zA-Z]+)(\d+)', d.name)
            if match:
                char, num = match.groups()
                if char in criteria and num not in criteria[char]:
                    try:
                        d.unlink()
                    except PermissionError as e:
                        print(f"Failed to remove {d}: {e}")
                else:
                    continue
            else:
                continue

def copy_unzip_and_process_cors_vrs_data(
        config, dgps_dir, cors_vrs_dir, reports_dir
):
    # copy each date_dir from used cors to cors_vrs
    used_cors_dir = utils.find_dir(
        dgps_dir, config['dir_patterns']['used_cors']
    )
    for sdir in used_cors_dir.iterdir():
        if sdir.is_dir() and  not sdir.name.startswith('.'):
            date = sdir.name[:2]
            date_dirpath = cors_vrs_dir / sdir.name
            utils.copy_dir(sdir, date_dirpath)
            # unzip leaves in date_dir
            utils.unzip_leaves(date_dirpath)
            # Logic to decide what files remain in date_dir
            pids = save_coords_file_get_pids(
                config, reports_dir, date_dirpath, sdir, date
            )
            if pids is None:
                continue
            pids = [id for id in pids if 'ibase' not in id.lower()]
            delete_older_files(config, date_dirpath, pids, reports_dir, date)

def output_cors_to_vrs(config, main_dir, cors_vrs_dir, dgps_dir, inputs):

    main_dir = Path(main_dir)
    # output cors or vrs dir
    cors_vrs_dirname = config.get('OUTPUT_SUBDIRS', [])[0]
    cors_vrs_dir = main_dir / cors_vrs_dirname
    # Add state coords file
    cors_known_dir = utils.find_dir(
        dgps_dir, config['dir_patterns']['known_coordinates']
    )
    coord_file = None
    for file in cors_known_dir.iterdir():
        if (inputs['state'].lower() in file.name.lower()):
            coord_file = file
            break
    if coord_file is None:
            raise ValueError()

    for dirname in cors_vrs_dir.iterdir():
        if dirname.is_dir():
            path = cors_vrs_dir / dirname
            coord_renamed = config['filename_templates']\
                ['known_cors_pdf'].format(state=inputs['state'])
            utils.copy_file(
                coord_file, path,
                renamed=coord_renamed
            )

def copy_and_flatten_base_rover_data(config, uav_dir, main_dir):

    uav_dir = Path(uav_dir)
    main_dir = Path(main_dir)

    ibase_dir = utils.find_dir(uav_dir, config['dir_patterns']['ibase_raw'])
    localbase_dirname = config.get('OUTPUT_SUBDIRS', [])[1]
    localbase_dir = ibase_dir / localbase_dirname

    # copy raw data and unzip recursively into new copied dirs
    for dir_ in ibase_dir.iterdir():
        if dir_.is_dir():
            flights_dir = localbase_dir / dir_.name
            utils.copy_dir(dir_, flights_dir)
            utils.unzip_leaves(flights_dir)

    # rover raw data
    rover_raw_dir = utils.find_dir(uav_dir, config['dir_patterns']['ppk_raw'])
    ppk_baro_dirname = config.get('OUTPUT_SUBDIRS', [])[2]
    ppk_baro_dir = main_dir / ppk_baro_dirname
    # copy folders into output directory and unzip all files recursively
    for dir_ in rover_raw_dir.iterdir():
        if dir_.is_dir():
            pockets_dir = ppk_baro_dir / dir_
            utils.copy_dir(dir_, pockets_dir)
            utils.flatten_dir(pockets_dir)

def get_geo_and_grid_data(config, dgps_dir):
    # Get geo and grid data
    geo_dir = utils.find_dir(dgps_dir, config['dir_patterns']['geo_format'])
    grid_dir = utils.find_dir(dgps_dir, config['dir_patterns']['grid_format'])

    geo_csv = next((f for f in geo_dir.glob('*.csv')), None)
    if geo_csv is None:
        raise FileNotFoundError(f"{geo_csv} not found in {geo_dir}")

    df_geo = utils.load_csv(geo_csv, delimiter='\t')
    df_geo.columns = config['table_config']['geo']['cols']
    grid_csv = next((f for f in grid_dir.glob('*.csv')), None)
    if grid_csv is None:
        raise FileNotFoundError(f"{grid_csv} not found in {grid_dir}.")
    df_grid = utils.load_csv(grid_csv, delimiter=',', skip=1)
    df_grid.columns = config['table_config']['grid']['cols']
    return df_geo, df_grid

def format_df_grid_data(config, df_grid):
    # sanity check for 'Easting' column
    df_grid['Easting'] = pd.to_numeric(df_grid['Easting'], errors='coerce')
    df_grid['Northing'] = pd.to_numeric(df_grid['Northing'], errors='coerce')

    if not df_grid['Easting'].between(1e5, 1e6).all():
        if df_grid['Northing'].between(1e5, 1e6).all():
            df_grid['Easting'], df_grid['Northing'] = (
                df_grid['Northing'], df_grid['Easting']
            )
        else:
            print('Warninig, check Easting column')

    df_grid = df_grid[config['table_config']['grid']['sorted_cols']]
    return df_grid


def create_checkpoint_coords_doc(config, main_dir, dgps_dir):

    ckpts_raw_dirname = config.get('OUTPUT_SUBDIRS', [])[3]
    ckpts_raw_dir = main_dir / ckpts_raw_dirname
    ckpt_docname = config['table_config']['doc_info']['docx_filename']
    ckpts_raw_path = ckpts_raw_dir / ckpt_docname


    df_geo, df_grid = get_geo_and_grid_data(config, dgps_dir)
    df_grid = format_df_grid_data(config, df_grid)

    templator = DocxTemplator()
    templator.set_save_path(ckpts_raw_path)
    # add report using class method
    templator.add_content(
        content_type='paragraph',
        content='Check Points Coordinates in Geodetic:',
        style={'alignment': 'left'}
    )
    templator.content.append(
        ('table', df_geo, {'cols': df_geo.columns}),
    )
    templator.add_content('page_break', '')

    templator.add_content(
        content_type='paragraph',
        content='Check Points Coordinates in UTM:',
        style={'alignment': 'left'}
    )
    templator.content.append(
        ('table', df_grid, {'cols': df_grid.columns})
    )
    # Generate the report
    templator.generate_report()
    # Save the document
    templator.save_doc()

    # add note file
    note = config['table_config']['doc_info']['note_content']
    notepath = ckpts_raw_dir / config['table_config']['doc_info']['note_name']

    with open(notepath, 'w') as note_ref:
        note_ref.write(note)

    return ckpts_raw_path

def copy_raw_images(config, uav_dir, main_dir):
    # copy raw images
    raw_img_dir = utils.find_dir(uav_dir, config['dir_patterns']['raw_images'])
    trg_img_dirname = config.get('OUTPUT_SUBDIRS', [])[4]

    for item in raw_img_dir.iterdir():
        trg = main_dir / trg_img_dirname / item

        if item.is_dir():
            utils.copy_dir(item, trg)
        else:
            utils.copy_file(item, trg)


def copy_geotagged_csv_and_images(uav_dir, main_dir, config):
    # copy geolocation files
    geotag_csv_dir = utils.find_dir(
        uav_dir, config['dir_patterns']['geotagged_csv']
    )
    geoloc_dirname = config.get('OUTPUT_SUBDIRS', [])[5]
    geoloc_dir = main_dir / geoloc_dirname

    for f in geotag_csv_dir.iterdir():
        if f.suffix == '.csv':
            match = re.search(r'\d+', f.name)
            if match:
                num = match.group()
                renamed = config['filename_templates']['geolocation_csv']
                renamed = renamed.format(num=num)
                utils.copy_file(f, geoloc_dir / renamed)

    # Geotagged Images
    tagged_imsrc_dir = utils.find_dir(
        uav_dir, config['dir_patterns']['geotagged_images']
    )

    geotag_imtrg_dir = main_dir / config.get('OUTPUT_SUBDIRS', [])[6]

    for d in tagged_imsrc_dir.iterdir():
        if d.is_dir():
            renamed = f'Geo{d.name}'
            utils.copy_dir(d, geotag_imtrg_dir / renamed)

# Functions to create DGNSS Report Template
def get_logsheet_points_path(config, dgps_dir):

    vil_logsheets_dir = utils.find_dir(
        dgps_dir, config['dir_patterns']['logsheet_points']
    )
    files = [
        f for f in vil_logsheets_dir.iterdir() if not f.name.startswith('.')
    ]
    if len(files) == 1:
        logsheet_path = files[0]
    else:
        logsheet_path = next(
            (f for f in files if inputs['village'].lower() in f.name.lower()
            and 'log' in f.name.lower()), None
        )
    return logsheet_path

def extract_report_sids(network_reports):
    # get name part after report name, before file extension
    pattern = re.compile(r' (\d+[^.]+)')
    s_ids = []
    for r in network_reports:
        match = pattern.search(r)
        if match:
            s_ids.append(match.group(1))
    return s_ids

def generate_all_dgnss_reports(config, s_ids, reports_dir, dgps_dir):
    for id in s_ids:
        net_paths = [
            os.path.join(reports_dir, r) for r in network_reports if id in r
        ]
        base_paths = [
            os.path.join(reports_dir, r) for r in baseline_reports if id in r
        ]
        for n, b in zip(net_paths, base_paths):
                net_reader = PDFTableReader(n)
                base_reader = PDFTableReader(b)
                net_table, col_heads = parse_network_table(net_reader, pgs=4)
                ibase_table = parse_ibase_reference_table(base_reader, pgs=4)

                summary_table = create_dgnss_summary(net_table, ibase_table)
                templator = DocxTemplator()
                dgnss_path = reports_dir / f"{config['dgnss']['doc_name']}_{id}.docx"
                templator.set_save_path(dgnss_path)
                configure_dgnss_content(templator, col_heads, summary_table, config['dgnss']['elements'])

                templator.generate_report()
                templator.save_doc()
                print(f"Report saved at: {dgnss_path}")

def configure_dgnss_content(templator, col_headers, summary_table, elements):
    for e in elements:
        if e['type'] == 'paragraph':
            templator.add_content(
                'paragraph',
                e['content'],
                e.get('style', {})
            )
        elif e['type'] == 'bold_prefix_paragraph':
            templator.add_content(
                'bold_prefix_paragraph',
                {'prefix': e['prefix'], 'content': e['content']},
                {'font_size': e['font_size'], 'font': e['font'], 'color': e['color']}
            )
        elif e['type'] == 'image':
            if e['path'] == 'dynamic':
                image_path = input("Please enter the path to the image: ")
                if not templator.try_add_image(image_path):
                    print(f"Image not found at {image_path}. Skipping image.")
            else:
                templator.add_content('image', e['path'])
        elif e['type'] == 'table':
            templator.add_content(
                'table',
                summary_table,
                {'columns': col_headers}
            )
        elif e['type'] == 'page_break':
            templator.add_content('page_break', None)

def parse_network_table(
        reader: PDFTableReader, pgs: int = 4
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Extracts the main table from the network report.
    """
    col_heads = reader.parse_col_vals(colname='point_id', unaligned=True)
    all_tables = reader.extract_tables(max_pages=pgs)
    table = reader.merge_tables(
        all_tables, key_col='point_id', unaligned=True
    )
    table['easting'] = table['easting\r(meter)'].astype(float)
    table['northing'] = table['northing\r(meter)'].astype(float)
    table['height'] = table['height\r(meter)'].astype(float)

    ibase_row = table[table['point id'].str.lower().str.contains('ibase')]
    ibase_row = ibase_row.iloc[0]

    table['ibase_dist'] = table.apply(
        lambda row: calc_dist(row, ibase_row),
        axis=1
    )
    # format ibase_dist column
    table['ibase_dist'] = table['ibase_dist'].apply(
        lambda x: (
            f'{x} km' if pd.notna(x) and x > 0
            else '-' if x ==0
            else None
        )
    )
    return table, col_heads

def calc_dist(row, ibase_row):
    d = utils.calculate_euclidean(
        (row['easting'], row['northing'], row['height']),
        (ibase_row['easting'], ibase_row['northing'], ibase_row['height'])
    )
    if d is not None:
        return round(d / 1000, 3)
    return None

def parse_ibase_reference_table(
        reader: PDFTableReader, pgs: int = 4
) -> pd.DataFrame:
    """
    Extracts the main table from baseline report for relevant rows
    """
    all_tables = reader.extract_tables(max_pages=pgs)
    base_df = all_tables[1]

    assert isinstance(base_df, pd.DataFrame), "base_df not a DataFrame."

    if base_df.empty:
        raise ValueError()

    base_df['From'] = base_df['From'].replace(r'\r', '', regex=True)
    base_df['To'] = base_df['To'].replace(r'\r', '', regex=True)

    # identify reference id
    rows_to_ibase = base_df[
        base_df['To'].str.contains('ibase', case=False, na=False)
    ]

    assert isinstance(rows_to_ibase, pd.DataFrame), "Expected a DataFrame"
    if rows_to_ibase.empty:
        raise ValueError("No rows with 'ibase' found in 'To' column")

    from_uniques = rows_to_ibase['From'].astype(str).unique()
    assert isinstance(from_uniques, (np.ndarray, list)), \
            "from_uniques must be an ndarray or list."
    if isinstance(from_uniques, np.ndarray):
            from_uniques = from_uniques.tolist()
    assert isinstance(from_uniques, list), "from_uniques converted to list."

    sol = base_df[base_df['From'].isin(from_uniques)]
    assert isinstance(sol, pd.DataFrame), "sol must be a DataFrame"
    # reference id is the max value in the sol df
    ref_id = sol['From'].value_counts().idxmax()

    # filter base_df
    ibase_df = base_df[base_df['From'] == ref_id].copy()
    ibase_df.set_index('To', inplace=True)
    assert isinstance(ibase_df, pd.DataFrame), "ibase_df not a DataFrame."

    return ibase_df

def create_dgnss_summary(
        net_table: pd.DataFrame,
        ibase_table: pd.DataFrame
) -> pd.DataFrame:
    """
    Creates a summary table for the DGNSS report.
    """
    assert isinstance(net_table, pd.DataFrame), "net_table not DataFrame."
    assert isinstance(ibase_table, pd.DataFrame), "ibase_table not DataFrame."

    res = pd.DataFrame()
    res['Point ID'] = net_table['point id']
    res['Latitude'] = net_table['latitude']
    res['Longitude'] = net_table['longitude']
    res['Ellipsoidal Height'] = net_table['height\r(meter)']
    res['Distance b/w Reference Station & IBASE'] = net_table['ibase_dist']

    res.dropna(axis=0, inplace=True)

    res['Horizontal Precision'] = 0
    res['Vertical Precision'] = 0

    ibase_table.index = ibase_table.index.astype(str)
    res['Point ID'] = res['Point ID'].astype(str)

    res['Horizontal Precision'] = res['Point ID'].apply(
            lambda pid: ibase_table['H. Prec.\r(Meter)'][pid]
            if pid in ibase_table.index else 0
        )
    res['Vertical Precision'] = res['Point ID'].apply(
        lambda pid: ibase_table['V. Prec.\r(Meter)'][pid]
        if pid in ibase_table.index else 0
    )

    # Handle the 'ibase' special case, find rows where 'ibase' appears in both 'point_id' and 'To'
    ibase_rows_res = res['point id'].str.contains('ibase', case=False)
    ibase_rows_df2 = ibase_table.index.str.contains('ibase', case=False)

    if ibase_rows_df2.any():
        ibase_match_idx = ibase_table.index[ibase_rows_df2][0]  # Assuming only one 'ibase' match
        res.loc[ibase_rows_res, 'Horizontal Precision'] = ibase_table.loc[ibase_match_idx, 'H. Prec.\r(Meter)']
        res.loc[ibase_rows_res, 'Vertical Precision'] = ibase_table.loc[ibase_match_idx, 'V. Prec.\r(Meter)']

    # adding units to precision columns
    res['Horizontal Precision'] = res['Horizontal Precision'].apply(
        lambda x: f'{x} m' if x != 0 else x
    )
    res['Vertical Precision'] = res['Vertical Precision'].apply(
        lambda x: f'{x} m' if x != 0 else x
    )
    res['point id'] = res['point id'].apply(
        lambda x: f'REFERENCE ({x})' if 'ibase' not in str(x).lower() else x
    )
    res = res.set_index('point id').transpose()
    res.columns.name = None
    return res



if __name__ == "__main__":
    config = utils.load_config('config.json')
    inputs = fetch_inputs(config)
    inputs_dir = Path(inputs['inputs_dir'])

    # Initialize the main output directory
    main_dir = initialize_output_dir_structure(config, inputs)
    # Get DGPS and Reports directories
    dgps_dir, reports_dir = get_dgps_and_reports_dir(config, inputs, main_dir)
    # baseline dir
    baseline_dir = utils.find_dir(dgps_dir, config['dir_patterns']['baseline'])
    # Move files to reports dir
    move_files_to_reports_dir(config, dgps_dir, baseline_dir, reports_dir)

    # Copy and Unzip CORS or VRS data, remove old files, save coords excel
    copy_unzip_and_process_cors_vrs_data(
        config, dgps_dir, main_dir, reports_dir
    )
    # output cors or vrs dir
    cors_vrs_dirname = config.get('OUTPUT_SUBDIRS', [])[0]
    cors_vrs_dir = main_dir / cors_vrs_dirname

    output_cors_to_vrs(config, main_dir, cors_vrs_dir, dgps_dir, inputs)
    # Local Base Raw Data
    uav_dir = utils.find_dir(inputs_dir, config['uav_report'])

    copy_and_flatten_base_rover_data(config, uav_dir, main_dir)
    # copy raw images
    copy_raw_images(config, uav_dir, main_dir)

    ckpts_raw_path = create_checkpoint_coords_doc(config, main_dir, dgps_dir)
    copy_geotagged_csv_and_images(uav_dir, main_dir, config)

    # dgnss report template

    network_reports = [d for d in os.listdir(reports_dir) if 'network' in d.lower()]
    baseline_reports = [d for d in os.listdir(reports_dir) if 'base' in d.lower()]
    reports_main = network_reports + baseline_reports
    s_ids = extract_report_sids(network_reports)
    generate_all_dgnss_reports(config, s_ids, reports_dir, dgps_dir)
    # automate other reports
