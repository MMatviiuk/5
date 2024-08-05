import asyncio
import aiofiles
import os
from pathlib import Path
from argparse import ArgumentParser
from loguru import logger
from typing import AsyncGenerator, Awaitable

# Configure logging
logger.add("debug.log", format="{time} {level} {message}", level="DEBUG", rotation="10 MB")

async def copy_file(source_path: Path, destination_folder: Path) -> None:
    """
    Asynchronously copies a file to a specified destination folder based on its extension.

    Args:
        source_path (Path): The path to the source file.
        destination_folder (Path): The path to the destination folder.
    """

    try:
        # Get the file extension without the leading dot
        extension = source_path.suffix[1:] or "no_extension"

        # Create the destination path using Path.join
        destination_path = destination_folder / extension / source_path.name

        # Create the destination directory if it doesn't exist
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        # Asynchronously copy the file
        async with aiofiles.open(source_path, 'rb') as src, \
                aiofiles.open(destination_path, 'wb') as dst:
            await dst.write(await src.read())

        logger.info(f"Copied {source_path} to {destination_path}")
    except Exception as e:
        logger.error(f"Error copying {source_path}: {e}")

async def read_folder(source_folder: str, output_folder: str) -> None:
    """
    Asynchronously reads all files in a source folder and its subfolders,
    copying them to the appropriate subfolders in the output folder based on their extensions.

    Args:
        source_folder (str): The path to the source folder.
        output_folder (str): The path to the output folder.
    """

    for root, _, files in os.walk(source_folder):
        tasks = []
        for file in files:
            source_path = Path(root) / file
            tasks.append(copy_file(source_path, Path(output_folder)))
        await asyncio.gather(*tasks)

async def main() -> None:
    """
    The main entry point of the program.
    """

    parser = ArgumentParser(description="Asynchronously sorts files by extension.")
    parser.add_argument('source_folder', type=str, help='Source folder to read files from.')
    parser.add_argument('output_folder', type=str, help='Destination folder to copy files to.')
    args = parser.parse_args()

    await read_folder(args.source_folder, args.output_folder)

if __name__ == '__main__':
    asyncio.run(main())
