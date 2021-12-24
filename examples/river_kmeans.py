import functools
import random
import time

import pandas as pd

from streamz import Stream
import hvplot.streamz
from streamz.river import RiverTrain
from river import cluster
import holoviews as hv
from panel.pane.holoviews import HoloViews
import panel as pn
hv.extension('bokeh')

model = cluster.KMeans(n_clusters=3, sigma=0.1, mu=0.5)
centres = [[random.random(), random.random()] for _ in range(3)]
count = [0]

def gen(move_chance=0.05):
    centre = int(random.random() * 3)  # 3x faster than random.randint(0, 2)
    if random.random() < move_chance:
        centres[centre][0] += random.random() / 5 - 0.1
        centres[centre][1] += random.random() / 5 - 0.1
    value = {'x': random.random() / 20 + centres[centre][0],
             'y': random.random() / 20 + centres[centre][1]}
    count[0] += 1
    return value


def get_clusters(model):
    # return [{"x": xcen, "y": ycen}, ...] for each centre
    data = [{'x': v['x'], 'y': v['y']} for k, v in model.centers.items()]
    return pd.DataFrame(data, index=range(3))


def main(viz=True):
    cadance = 0.01

    ex = pd.DataFrame({'x': [0.5], 'y': [0.5]})
    pipe_in = hv.streams.Pipe(data=ex)
    pipe_out = hv.streams.Pipe(data=ex)

    # setup pipes
    s = Stream.from_periodic(gen, cadance)

    # Branch 0: Input/Observations
    obs = s.map(lambda x: pd.DataFrame([x]))

    # Branch 1: Output/River ML clusters
    km = RiverTrain(model, pass_model=True)
    s.map(lambda x: (x,)).connect(km)  # learn takes a tuple of (x,[ y[, w]])
    clusters = km.map(get_clusters)

    concat = functools.partial(pd.concat, ignore_index=True)

    def accumulate(previous, new, last_lines=50):
        return concat([previous, new]).iloc[-last_lines:, :]

    partition_obs = 10
    particion_clusters = 10
    backlog_obs = 100

    # .partition is used to gather x number of points
    # before sending them to the plots
    # .accumulate allows to generate a backlog

    (
        obs
        .partition(partition_obs)
        .map(concat)
        .accumulate(functools.partial(accumulate, last_lines=backlog_obs))
        .sink(pipe_in.send)
    )
    (
        clusters
        .partition(particion_clusters)
        .map(pd.concat)
        .sink(pipe_out.send)
    )

    # start things
    s.emit(gen())  # set initial model
    for i, (x, y) in enumerate(centres):
        model.centers[i]['x'] = x
        model.centers[i]['y'] = y

    print("starting")

    if viz:
        # plot
        button_start = pn.widgets.Button(name='Start')
        button_stop = pn.widgets.Button(name='Stop')

        t0 = 0

        def start(event):
            s.start()
            global t0
            t0 = time.time()

        def stop(event):
            print(count, "events")
            global t0
            t_spent = time.time() - t0
            print("frequency", count[0] / t_spent, "Hz")
            print("Current centres", centres)
            print("Output centres", [list(c.values()) for c in model.centers.values()])
            s.stop()

        button_start.on_click(start)
        button_stop.on_click(stop)

        scatter_dmap_input = hv.DynamicMap(hv.Scatter, streams=[pipe_in]).opts(color="blue")
        scatter_dmap_output = hv.DynamicMap(hv.Scatter, streams=[pipe_out]).opts(color="red")
        pl = scatter_dmap_input * scatter_dmap_output
        pl.opts(xlim=(-0.2, 1.2), ylim=(-0.2, 1.2), height=600, width=600)

        pan = HoloViews(pl)
        app = pn.Row(pn.Column(button_start, button_stop), pan)
        app.show()
    else:
        s.start()
        time.sleep(5)
        print(count, "events")
        print("frequency", count[0] / 5, "Hz")
        print("Current centres", centres)
        print("Output centres", [list(c.values()) for c in model.centers.values()])
        s.stop()

if __name__ == "__main__":
    main(viz=True)
