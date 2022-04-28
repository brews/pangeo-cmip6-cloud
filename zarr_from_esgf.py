import sys
import os
import logging
import tempfile
from typing import Sequence
from pathlib import Path

from fsspec.implementations.local import LocalFileSystem
from fsspec.core import url_to_fs
from pangeo_forge_recipes.storage import (
    FSSpecTarget,
    CacheFSSpecTarget,
    MetadataTarget,
    StorageConfig,
)
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

from mysearch import (
    esgf_search,
)  # We probably want to strip this out later, left as is for now.


def get_urls(dataset_id: str) -> Sequence[str]:
    # CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.so.gn.v20190429
    facet_labels = (
        "mip_era",
        "activity_id",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
        "version",
    )

    facet_vals = dataset_id.split(".")
    if len(facet_vals) != 10:
        raise ValueError(
            "Please specify a query of the form {"
            + ("}.{".join(facet_labels).upper())
            + "}"
        )

    facets = dict(zip(facet_labels, facet_vals))

    if facets["mip_era"] != "CMIP6":
        raise ValueError("Only CMIP6 mip_era supported")

    # version is still not working
    # if facets["version"].startswith("v"):
    #    facets["version"] = facets["version"][1:]

    node_dict = {
        "llnl": "https://esgf-node.llnl.gov/esg-search/search",
        "ipsl": "https://esgf-node.ipsl.upmc.fr/esg-search/search",
        "ceda": "https://esgf-index1.ceda.ac.uk/esg-search/search",
        "dkrz": "https://esgf-data.dkrz.de/esg-search/search",
    }

    # version doesn't work here
    keep_facets = (
        "activity_id",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
    )
    search_facets = {f: facets[f] for f in keep_facets}

    search_node = "llnl"
    ESGF_site = node_dict[
        search_node
    ]  # TODO: We might have to be more clever here and search through different nodes. For later.

    df = esgf_search(search_facets, server=ESGF_site)  # this modifies the dict inside?

    # get list of urls
    urls = df["url"].tolist()

    # sort urls in decending time order (to be able to pass them directly to the pangeo-forge recipe)
    end_dates = [url.split("-")[-1].replace(".nc", "") for url in urls]
    urls = [url for _, url in sorted(zip(end_dates, urls))]
    # TODO Check that there are no gaps or duplicates.

    return urls


def main(dataset_id: str, *, target_url: str, prune: bool = False) -> None:
    pattern = pattern_from_file_sequence(get_urls(dataset_id), "time")

    with tempfile.TemporaryDirectory() as tmpdirname:

        fs_local = LocalFileSystem()
        storageconf = StorageConfig(
            target=FSSpecTarget(*url_to_fs(target_url)),
            cache=CacheFSSpecTarget(fs_local, Path(tmpdirname).joinpath("cache")),
            metadata=MetadataTarget(fs_local, Path(tmpdirname).joinpath("metadata")),
        )

        recipe = XarrayZarrRecipe(
            pattern,
            target_chunks={"time": 3},
            xarray_concat_kwargs={"join": "exact"},
            storage_config=storageconf,
        )

        if prune:
            # For test runs and such.
            recipe.copy_pruned().to_function()()
        else:
            recipe.to_function()()

        print(recipe.target)


if __name__ == "__main__":
    # Parse user-input arguments, configs.
    dataset_id = sys.argv[1]  # Like "CMIP6.DAMIP.CSIRO-ARCCSS.ACCESS-CM2.hist-nat.r1i1p1f1.day.tas.gn.v20201120"
    outurl = sys.argv[2]  # Like "/tmp/output.zarr", or an fsspec-readable URL.

    # Check environment variables. If ZARRFROMESGF_PRUNE is set to something 
    # truthy then doing a "pruned" test run that doesn't grab all data from 
    # external server.
    prune = os.environ.get("ZARRFROMESGF_PRUNE", False)
    if prune and prune.lower() in ("0", "false"):
        prune = False
    prune = bool(prune)

    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logger = logging.getLogger("pangeo_forge_recipes")
    logger.setLevel(logging.INFO)

    main(
        dataset_id, target_url=outurl, prune=prune
    )
