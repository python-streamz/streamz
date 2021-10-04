import random

import pandas as pd
import panel.pane.holoviews
import tornado.ioloop

from streamz import Stream
import hvplot.streamz
from streamz.river import RiverTrain
from river import cluster
import holoviews as hv
import panel as pn
from panel.pane.holoviews import HoloViews
import tornado.ioloop
hv.extension('bokeh')

model = cluster.STREAMKMeans(n_clusters=3)
centres = [[random.random(), random.random()] for _ in range(3)]

def gen(split_chance=0.01):
    centre = int(random.random() * 3)  # 3x faster than random.randint(0, 2)
    if random.random() < split_chance:
        centres[centre] = [random.random(), random.random()]
    value = {'x': random.random() / 20 + centres[centre][0],
             'y': random.random() / 20 + centres[centre][1]}
    return value


def get_clusters(model):
    # return [{"x": xcen, "y": ycen}, ...] for each centre
    data = [{'x': v['x'], 'y': v['y']} for k, v in model.centers.items()]
    return pd.DataFrame(data, index=range(3))


def main():
    # setup pipes
    s = Stream.from_periodic(gen, 0.05)
    km = RiverTrain(model, pass_model=True)
    s.map(lambda x: (x,)).connect(km)  # learn takes a tuple of (x,[ y[, w]])
    ex = pd.DataFrame({'x': [0.5], 'y': [0.5]})
    ooo = s.map(lambda x: pd.DataFrame([x])).to_dataframe(example=ex)
    out = km.map(get_clusters)

    # start things
    s.emit(gen())  # set initial model
    for i, (x, y) in enumerate(centres):
        model.centers[i]['x'] = x
        model.centers[i]['y'] = y

    s.start()

    # plot
    pout = out.to_dataframe(example=ex)
    pl = (ooo.hvplot.scatter('x', 'y', color="blue", backlog=100) *
          pout.hvplot.scatter('x', 'y', color="red", backlog=3))
    pl.opts(xlim=(-0.5, 1.5), ylim=(-0.5, 1.5), height=600, width=600)
    pan = panel.pane.holoviews.HoloViews(pl)
    pan.show()


if __name__ == "__main__":
    main()
