import zipfile
from zipfile import ZipFile, is_zipfile
import io, os

class ZipExtractor:
    """
    A class to extract data from a zip file and save locally.

    Args:
        zip_object (BytesIO): name of gcp project
        folder_file_path (str): folder path name in local directory
    """

    def __init__(self, zip_object: io.BytesIO, folder_file_path: str):
        self.zip_object = zip_object
        self.folder_file_path = folder_file_path
        assert self._check_is_zipfile() == True

    def _check_is_zipfile(self) -> bool:
        """Checks if a zip file object is actually a zip file object

        Return:
            bool: True or False dependent on success
        """

        if is_zipfile(self.zip_object):
            return True

        return False
    
    def unzip_object(self) -> io.BytesIO:
        """
        return Zip object with provided tools to create, read, write, append, and list a ZIP file
        Return:
            BytesIO: 
        """
        return ZipFile(self.zip_object, "r")
    
    def write_string_to_file(self, file: str, string: str):
        """
        open file and write string contents into a file in directory under file_name
        Args:
            file (str): directory location and name of file to write contents to
            string (str): string contents to write into file (overwrite)
        """
        with open(file, "w", encoding="utf-8") as md_file:
            md_file.write(string)
    
    def write_bytes_to_binary_file(self, file_bytes: io.BytesIO, file_name: str):
        """
        open file and write bytes to a binary file in directory under file_name
        Args:
            file_name (str): directory location and name of file to write contents to
            file_bytes (BytesIO):: string contents to write into file (overwrite)
        """
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "wb") as binary_file:
            binary_file.write(file_bytes)

    def list_extracted_files(self) -> list:
        """
        Extract and list extracted files
        Return:
            list: list of files within the zipfile
        """
        return ZipFile(self.zip_object, "r").namelist()