from typing import List
from functools import reduce
import pandas as pd
from tabula.io import read_pdf
import re
import PyPDF2


class PDFTableReader:

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path

    def __repr__(self) -> str:
        return f"PDFTableReader(pdf_path='{self.pdf_path}')"

    def __len__(self) -> int:
        return len(self.extract_tables())

    def __getitem__(self, index: int) -> pd.DataFrame:
        tables = self.extract_tables()
        if index < len(tables):
            return tables[index]
        else:
            raise IndexError("Index out of range")

    def extract_tables(
            self, max_pages: int = 4
    ) -> List[pd.DataFrame]:
        with open(self.pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            num_pages = len(reader.pages)

        pages = min(max_pages, num_pages)

        dfs = read_pdf(
            self.pdf_path, pages=f'1-{pages}', multiple_tables=True,
            lattice=True, pandas_options={'dtype': str}
        )
        if not isinstance(dfs, list):
                raise ValueError("Expected a list of DataFrames.")
        return dfs

    def clean_and_align_dfs(self, dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
        """
        Cleans and aligns the DataFrames extracted from the PDF.
        """
        aligned = []
        for df in dfs:
            df.columns = df.columns.str.lower().str.strip()
            df_copy = df.copy()
            cols = df_copy.columns[1:]
            df_copy = df_copy.iloc[:, :-1]
            df_copy.columns = cols
            aligned.append(df_copy)

        clean = [
            df.dropna(axis=0, how='all').loc[:, ~df.columns.str.startswith('unnamed')]
            for df in aligned
        ]
        return clean

    def merge_tables(
        self, dfs: List[pd.DataFrame], key_col: str, unaligned: bool=True
    )-> pd.DataFrame:

        dfs = dfs[:7]
        if unaligned:
            dfs = self.clean_and_align_dfs(dfs)
        filtered = [df for df in dfs if key_col in df.columns]
        merged = reduce(
            lambda l, r: pd.merge(l, r, on=key_col, how='outer'), filtered
        )
        return merged

    def parse_col_vals(
            self, colname: str, unaligned: bool=True) -> List[str]:

        dfs = self.extract_tables()
        merged = self.merge_tables(
            dfs=dfs, key_col=colname, unaligned=unaligned
        )
        merged.dropna(axis=0, how='all', inplace=True)

        col_vals = merged[colname].unique().tolist()

        vals = [val for val in col_vals if not re.search(r'\d', val) or 'base' in val.lower()]

        return vals

    def remove_nums_in_col(
            self, df: pd.DataFrame, col: str
    )-> pd.DataFrame:
        """Removes rows with numbers in the col"""

        if df.empty:
            return pd.DataFrame()
        if col not in df.columns:
            raise ValueError(f"{col} does not exist in {df}")
        df[col] = df[col].astype(str)
        no_nums = ~df[col].str.contains(r'\d', regex=True)
        filtered = df.loc[no_nums]

        assert isinstance(filtered, pd.DataFrame), "Expected a DataFrame to be returned."
        return filtered

    def extract_latest_filenames(
            self, keywords: List[str], max_pages: int = 4
    )-> List[str]:
        dfs = self.extract_tables(max_pages=max_pages)
        if dfs:
            last_df = dfs[-1]
            latest_files = []
            for _, row in last_df.iterrows():
                # select rows with keyword in the first column
                if pd.notna(row.iloc[0]) and any(kw in row.iloc[0].lower() for kw in keywords):
                    files = [row.iloc[i] for i in range(1, len(row)) if pd.notna(row.iloc[i])]
                    latest_files.extend([f.split('\\')[-1] for f in files])
            return latest_files
        return []
