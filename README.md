# bovra

A code for predicting time-series of traffic in different cities. The core of the
code implements a Graph Neural Network (GNN) to predict urban traffic density.
GNNs allow the model to capture temporal dynamics across spatial locations. The
project explores two flavors of spatial-temporal GNNs: Diffusion Convolutional
Recurrent Neural Networks (DCRNN) and Graph WaveNet for Deep Spatial-Temporal
Graph Modeling. In addition, the repository provides a small ETL layer that
ingests and filters traffic data obtained from the Madrid Open Data portal so
that these models can be trained on clean, ready-to-use datasets.

## Repository structure

- `main.py` – a lightweight CLI entrypoint (currently prints a greeting) that can
  later be extended to orchestrate training, evaluation, and preprocessing
  pipelines.
- `conf/` – top-level Hydra configuration. The default `config.yaml` defines
  logging preferences, dataset sources, and basic model sequence lengths.
- `dataset/` – data preparation scripts and their own Hydra configuration
  (`dataset/conf/dataset.yaml`) that describe where raw CSVs live, where to
  materialize the processed data, and the names of intermediate files.
- `data/` – placeholder directory for generated artifacts (filtered historic
  CSVs, datasets, and distance matrices).
- `models/` – implementations of the various graph neural networks (DCRNN,
  Graph WaveNet, and STAWnet). Each model subdirectory can be expanded with
  training and evaluation routines.

## Requirements

- Python 3.13 or newer (the project already targets `>=3.13` in `pyproject.toml`).
- Stable dependencies listed in `requirements.txt` / `pyproject.toml`, including
  `torch`, `tensorflow`, `hydra-core`, `omegaconf`, `pandas`, `numpy`, `rich`,
  `statsmodels`, `matplotlib`, and others.

Install the project and its pinned dependencies through `uv`, which handles
virtual environments and dependency management together.

```sh
uv sync
```

## Configuration

- `conf/config.yaml` sets the directory where Hydra writes its run outputs and
  defines base dataset parameters such as the source HDF dataset, sequence
  lengths, and whether to use day-of-week features.
- `dataset/conf/dataset.yaml` is dedicated to the ETL scripts. It contains the
  raw CSV locations (e.g., `/home/adelacalle/Code/bovra/data/intensidad_trafico`),
  the results folder, file names for lists, locations, and distance matrices,
  and both `start_date`/`end_date` and the dataset pivot column (`vmed`). Each
  ETL script reads from this configuration via Hydra.

When invoking any Hydra-managed script from `dataset/`, pass `dataset/conf` as
the config path (Hydra will already default to it via the decorator). Example::

```sh
python -m dataset.getting_m30_pmed
python -m dataset.extract_pmed_from_historic
python -m dataset.create_dataset
python -m dataset.getting_sensor_graph_info
```

Hydra writes outputs to the `results` directory defined in the dataset config
(`/home/adelacalle/Code/bovra/data` by default). Files produced during the
pipeline include:

- `pts_med_M30.txt` – the filtered sensor ID list.
- `pmed_locations.csv` – metadata about the selected M30 sensors.
- `historic_M30.csv` – filtered historic traffic for the selected sensors.
- `dataset_M30.h5` – the pivoted dataset indexed by timestamp and sensor ID.
- `graph_sensor_ids.txt`, `graph_sensor_locations.csv`, `distances_m30_2019.csv`
  – auxiliary files used when constructing graph structures for training.

## Dataset preparation workflow

1. **Select M30 sensors** – `dataset/getting_m30_pmed.py` scans raw CSVs for the
   `M30` measurement type, writes the selected sensor list, locations, and the
   initial distance matrix.
2. **Extract historic PMED data** – `dataset/extract_pmed_from_historic.py`
   filters historic CSV months to the sensors selected in step 1 and writes a
   unified CSV (`historic_M30.csv`).
3. **Create dataset** – `dataset/create_dataset.py` reads the filtered CSV, pivots
   the data into a `timestamp × sensor` table, and stores it in an HDF5 file.
4. **Sensor graph data** – `dataset/getting_sensor_graph_info.py` updates sensor
   metadata files, records the selected sensor IDs in order, and recomputes a
   clean pairwise distance matrix in case location filtering changes.

Each script logs progress via Python's `logging` module and its own `rich`
progress bars where appropriate.

## Model implementations

- `models/DCRNN/` – Diffusion Convolutional Recurrent Neural Network implementation.
- `models/Graph-WaveNet/` – Graph WaveNet for spatial-temporal modeling.
- `models/STAWnet/` – Spatio-temporal attention-based networks.

These subprojects contain training/graph utilities that can be wired to the
datasets produced above. Look into each folder for model-specific instructions
and dependency requirements.

## Next steps

1. Extend `main.py` into a CLI or training orchestration layer that ties
   together configuration, preprocessing, and model training.
2. Add documentation for each model directory so contributors know how to run
   training/validation scripts (e.g., expected dataset, checkpointing).
3. Commit the `data/` artifacts or provide a script to download the Madrid Open
   Data CSVs so the ETL pipeline can run end-to-end.

## License & contribution

Use the repository as a foundation for researching spatial-temporal traffic
prediction. Add your own license header or contribution guidelines as the
project grows.
