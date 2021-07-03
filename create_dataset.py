import os
import logging
from omegaconf import DictConfig
import hydra
import numpy as np
import pandas as pd
import tables


log = logging.getLogger(__name__)


def generate_graph_seq2seq_io_data(
        df, x_offsets, y_offsets, add_time_in_day=True, add_day_in_week=False, scaler=None
):
    """
    Generate samples from
    :param df:
    :param x_offsets:
    :param y_offsets:
    :param add_time_in_day:
    :param add_day_in_week:
    :param scaler:
    :return:
    # x: (epoch_size, input_length, num_nodes, input_dim)
    # y: (epoch_size, output_length, num_nodes, output_dim)
    """

    num_samples, num_nodes = df.shape
    data = np.expand_dims(df.values, axis=-1)
    feature_list = [data]
    if add_time_in_day:
        time_ind = (df.index.values - df.index.values.astype("datetime64[D]")) / np.timedelta64(1, "D")
        time_in_day = np.tile(time_ind, [1, num_nodes, 1]).transpose((2, 1, 0))
        feature_list.append(time_in_day)
    if add_day_in_week:
        dow = df.index.dayofweek
        dow_tiled = np.tile(dow, [1, num_nodes, 1]).transpose((2, 1, 0))
        feature_list.append(dow_tiled)

    data = np.concatenate(feature_list, axis=-1)
    x, y = [], []
    min_t = abs(min(x_offsets))
    max_t = abs(num_samples - abs(max(y_offsets)))  # Exclusive
    for t in range(min_t, max_t):  # t is the index of the last observation.
        x.append(data[t + x_offsets, ...])
        y.append(data[t + y_offsets, ...])
    x = np.stack(x, axis=0)
    y = np.stack(y, axis=0)
    return x, y

def create_offsets(seq_length_x: int, seq_length_y:int, y_start: int):
    # 0 is the latest observed sample.
    x_offsets = np.sort(np.concatenate((np.arange(-(seq_length_x - 1), 1, 1),)))
    # Predict the next one hour
    y_offsets = np.sort(np.arange(y_start, (seq_length_y + 1), 1))
    return x_offsets, y_offsets


def split_dataset(x, y, train_split:float = 0.7, test_split:int = 0.2):
    num_samples = x.shape[0]
    num_test = round(num_samples * test_split)
    num_train = round(num_samples * train_split)
    num_val = num_samples - num_test - num_train
    modes = ['train', 'val', 'test']
    num_splits = [[0, num_train], [num_train, num_train+num_val], [-num_test, num_samples]]
    X = {mode: x[start:end] for mode, (start, end) in zip(modes, num_splits)}
    Y = {mode: y[start:end] for mode, (start, end) in zip(modes, num_splits)}
    return X, Y


@hydra.main(config_path="conf", config_name="config")
def create_dataset(cfg: DictConfig) -> None:
    
    log.info('Creating dataset...')
    if os.path.exists(cfg.output_folder):
        reply = str(input(f'{cfg.output_folder} exists. Do you want to overwrite it? (y/n)')).lower().strip()
        if reply[0] != 'y': exit
    else:
        os.makedirs(args.output_folder)

    seq_length_x, seq_length_y = cfg.seq_length_x, cfg.seq_length_y
    if os.path.exists(cfg.source_df):
        df = pd.read_hdf(cfg.source_df)
    else:
        raise FileNotFoundError('Source dataframe not found.')
    
    log.info("Generating sequences...")
    x_offsets, y_offsets = create_offsets(seq_length_x, seq_length_y, cfg.y_start)
    # Expected shape of input / output:
    # x: (num_samples, input_length, num_nodes, input_dim)
    # y: (num_samples, output_length, num_nodes, output_dim)
    x, y = generate_graph_seq2seq_io_data(
        df,
        x_offsets=x_offsets,
        y_offsets=y_offsets,
        add_time_in_day=True,
        add_day_in_week=cfg.dow,
    )

    log.info("Input/Output shapes:")
    log.info(f'X shape: {x.shape}')
    log.info(f'Y shape: {y.shape}')
    
    # Split dataset
    log.info('Splitting dataset...')
    X, Y = split_dataset(x, y)

    # Write the data into npz file.
    log.info("Train/Val/Test shapes:")
    for cat in ["train", "val", "test"]:
        log.info(f"x {cat}: {X[cat].shape}")
        log.info(f"y {cat}: {Y[cat].shape}")
        np.savez_compressed(
            os.path.join(cfg.output_folder, f"{cat}.npz"),
            x=X[cat], y= Y[cat],
            x_offsets=x_offsets[:, np.newaxis],
            y_offsets=y_offsets[:, np.newaxis])

    log.info("Finished!")

if __name__ == "__main__":
    create_dataset()
