import holoviews as hv
import pandas as pd

from holoviews.core.spaces import DynamicMap
from holoviews.core.overlay import NdOverlay
from holoviews.element import (
    Curve, Scatter, Area, Bars, BoxWhisker, Dataset, Distribution,
    Table
)

from holoviews.operation import histogram
from holoviews.streams import Buffer, Pipe

from streamz.dataframe import DataFrame, DataFrames, Series, Seriess


class HoloViewsConverter(object):

    def __init__(self, data, kind=None, by=None, width=800,
                 height=300, backlog=1000, shared_axes=False,
                 grid=False, legend=True, rot=None, title=None,
                 xlim=None, ylim=None, xticks=None, yticks=None,
                 fontsize=None, colormap=None, stacked=False,
                 logx=False, logy=False, loglog=False, hover=False,
                 **kwds):

        # Set up HoloViews stream
        if isinstance(data, (Series, Seriess)):
            data = data.to_frame()
        self.data = data
        self.stream_type = data._stream_type
        if data._stream_type == 'updating':
            self.stream = Pipe(data=data.example)
        else:
            self.stream = Buffer(data.example, length=backlog)
        data.stream.sink(self.stream.send)

        # High-level options
        self.by = by
        self.stacked = stacked
        self.kwds = kwds

        # Process style options
        if 'cmap' in kwds and colormap:
            raise TypeError("Only specify one of `cmap` and `colormap`.")
        elif 'cmap' in kwds:
            cmap = kwds.pop('cmap')
        else:
            cmap = colormap
        self._style_opts = {'fontsize': fontsize, 'cmap': cmap}

        # Process plot options
        plot_options = {}
        plot_options['logx'] = logx or loglog
        plot_options['logy'] = logy or loglog
        plot_options['show_grid'] = grid
        plot_options['shared_axes'] = shared_axes
        plot_options['show_legend'] = legend
        if xticks:
            plot_options['xticks'] = xticks
        if yticks:
            plot_options['yticks'] = yticks
        if width:
            plot_options['width'] = width
        if height:
            plot_options['height'] = height
        if rot:
            if (kind == 'barh' or kwds.get('orientation') == 'horizontal'
                or kwds.get('vert')):
                axis = 'yrotation'
            else:
                axis = 'xrotation'
            plot_options[axis] = rot
        if hover:
            plot_options['tools'] = ['hover']
        self._plot_opts = plot_options

        self._relabel = {'label': title}
        self._dim_ranges = {'x': xlim or (None, None),
                            'y': ylim or (None, None)}
        self._norm_opts = {'framewise': True}

    def reset_index(self, data):
        if self.stream_type == 'updating':
            return data.reset_index()
        else:
            return data

    def table(self, x=None, y=None):
        allowed = ['width', 'height']
        def table(data):
            if len(data.columns) == 1:
                data = data.reset_index()
            return Table(data).opts(plot=opts)
        opts = {k: v for k, v in self._plot_opts.items() if k in allowed}
        return DynamicMap(table, streams=[self.stream])


class HoloViewsFrameConverter(HoloViewsConverter):
    """
    HoloViewsFrameConverter handles the conversion of
    streamz.dataframe.DataFrame and streamz.dataframe.DataFrames into
    displayable HoloViews objects.
    """

    def __call__(self, kind, x, y):
        return getattr(self, kind)(x, y)

    def single_chart(self, chart, x, y):
        opts = dict(plot=self._plot_opts, norm=self._norm_opts)
        ranges = {x: self._dim_ranges['x'], y: self._dim_ranges['y']}

        def single_chart(data):
            return (chart(self.reset_index(data), x, y).redim.range(**ranges)
                    .relabel(**self._relabel).opts(**opts))

        return DynamicMap(single_chart, streams=[self.stream])

    def chart(self, element, x, y):
        "Helper method for simple x vs. y charts"

        if x and y:
            return self.single_chart(element, x, y)

        x = self.data.example.index.name or 'index'
        opts = dict(plot=dict(self._plot_opts, labelled=['x']),
                    norm=self._norm_opts)
        def multi_chart(data):
            charts = {}
            for c in data.columns[1:]:
                chart = element(data, x, c)
                ranges = {x: self._dim_ranges['x'], c: self._dim_ranges['y']}
                charts[c] = (chart.relabel(**self._relabel)
                             .redim.range(**ranges).opts(**opts))
            return NdOverlay(charts)
        return DynamicMap(multi_chart, streams=[self.stream])

    def line(self, x, y):
        return self.chart(Curve, x, y)

    def scatter(self, x, y):
        return self.chart(Scatter, x, y)

    def area(self, x, y):
        areas = self.chart(Area, x, y)
        if self.stacked:
            areas = areas.map(Area.stack, NdOverlay)
        return areas

    def bars(self, x, y):
        if x and y:
            return self.single_chart(Bars, x, y)

        index = self.data.example.index.name or 'index'
        stack_index = 1 if self.stacked else None
        opts = {'plot': dict(self._plot_opts, labelled=['x'],
                             stack_index=stack_index),
                'norm': self._norm_opts}
        ranges = {'Value': self._dim_ranges['y']}

        def bars(data):
            data = self.reset_index(data)
            df = pd.melt(data, id_vars=[index], var_name='Group', value_name='Value')
            return (Bars(df, [index, 'Group'], 'Value').redim.range(**ranges)
                    .relabel(**self._relabel).opts(**opts))
        return DynamicMap(bars, streams=[self.stream])

    def barh(self, x, y):
        return self.bars(x, y).opts(plot={'Bars': dict(invert_axes=True)})

    def box(self, x, y):
        if x and y:
            return self.single_chart(BoxWhisker, x, y)

        index = self.data.example.index.name or 'index'
        if self.by:
            id_vars = [index, self.by]
            kdims = ['Group', self.by]
        else:
            kdims = ['Group']
            id_vars = [index]
        invert = not self.kwds.get('vert', True)
        opts = {'plot': dict(self._plot_opts, labelled=[], invert_axes=invert),
                'norm': self._norm_opts}
        ranges = {'Value': self._dim_ranges['y']}

        def box(data):
            data = self.reset_index(data)
            df = pd.melt(data, id_vars=id_vars, var_name='Group', value_name='Value')
            return (BoxWhisker(df, kdims, 'Value').redim.range(**ranges)
                    .relabel(**self._relabel).opts(**opts))
        return DynamicMap(box, streams=[self.stream])

    def hist(self, x, y):
        plot_opts = dict(self._plot_opts)
        invert = self.kwds.get('orientation', False) == 'horizontal'
        opts = dict(plot=dict(plot_opts, labelled=['x'], invert_axes=invert),
                    style=dict(alpha=self.kwds.get('alpha', 1)),
                    norm=self._norm_opts)
        hist_opts = {'num_bins': self.kwds.get('bins', 10),
                     'bin_range': self.kwds.get('bin_range', None),
                     'normed': self.kwds.get('normed', False)}

        def hist(data):
            ds = Dataset(self.reset_index(data))
            hists = {}
            for col in data.columns[1:]:
                hist = histogram(ds, dimension=col, **hist_opts)
                ranges = {hist.vdims[0].name: self._dim_ranges['y']}
                hists[col] = (hist.redim.range(**ranges)
                              .relabel(**self._relabel).opts(**opts))
            return NdOverlay(hists)
        return DynamicMap(hist, streams=[self.stream])

    def kde(self, x, y):
        index = self.data.example.index.name or 'index'
        plot_opts = dict(self._plot_opts)
        invert = self.kwds.get('orientation', False) == 'horizontal'
        opts = dict(plot=dict(plot_opts, invert_axes=invert),
                    style=dict(alpha=self.kwds.get('alpha', 0.5)),
                    norm=self._norm_opts)
        opts = {'Distribution': opts, 'Area': opts,
                'NdOverlay': {'plot': dict(legend_limit=0)}}

        def kde(data):
            data = self.reset_index(data)
            df = pd.melt(data, id_vars=[index], var_name='Group', value_name='Value')
            ds = Dataset(df)
            if len(df):
                overlay = ds.to(Distribution, 'Value').overlay()
            else:
                overlay = NdOverlay({0: Area([], 'Value', 'Value Density')},
                                    ['Group'])
            return overlay.relabel(**self._relabel).opts(**opts)
        return DynamicMap(kde, streams=[self.stream])


class HoloViewsSeriesConverter(HoloViewsConverter):
    """
    HoloViewsFrameConverter handles the conversion of
    streamz.dataframe.Series and streamz.dataframe.Seriess into
    displayable HoloViews objects.
    """


    def __call__(self, kind):
        return getattr(self, kind)()

    def chart(self, chart):
        opts = dict(plot=self._plot_opts, norm=self._norm_opts)

        def chartfn(data):
            if len(data.columns) == 1:
                data = data.reset_index()
            return chart(data).relabel(**self._relabel).opts(**opts)

        return DynamicMap(chartfn, streams=[self.stream])

    def line(self):
        return self.chart(Curve)

    def scatter(self):
        return self.chart(Scatter)

    def area(self):
        return self.chart(Area)

    def bars(self):
        return self.chart(Bars)

    def barh(self, x, y):
        return self.bars().opts(plot={'Bars': dict(invert_axes=True)})

    def box(self):
        opts = dict(plot=self._plot_opts, norm=self._norm_opts)

        def boxfn(data):
            ranges = {data.columns[-1]: self._dim_ranges['y']}
            return (BoxWhisker(data, [], data.columns[-1])
                    .redim.range(**ranges).relabel(**self._relabel).opts(**opts))

        return DynamicMap(boxfn, streams=[self.stream])

    def hist(self):
        hist_opts = {'num_bins': self.kwds.get('bins', 10),
                     'bin_range': self.kwds.get('bin_range', None),
                     'normed': self.kwds.get('normed', False)}

        invert = self.kwds.get('orientation', False) == 'horizontal'
        opts = dict(plot=dict(self._plot_opts, invert_axes=invert),
                    style=dict(alpha=self.kwds.get('alpha', 1)),
                    norm=self._norm_opts)
        def hist(data):
            ds = Dataset(data)
            hist = histogram(ds, dimension=data.columns[-1], **hist_opts)
            ranges = {hist.vdims[0].name: self._dim_ranges['y']}
            return hist.redim.range(**ranges).relabel(**self._relabel).opts(**opts)
        return DynamicMap(hist, streams=[self.stream])

    def kde(self):
        invert = self.kwds.get('orientation', False) == 'horizontal'
        opts = dict(plot=dict(self._plot_opts, invert_axes=invert),
                    style=dict(alpha=self.kwds.get('alpha', 1)),
                    norm=self._norm_opts)

        def distfn(data):
            ranges = {data.columns[-1]: self._dim_ranges['y']}
            return (Distribution(data, data.columns[-1])
                    .redim.range(**ranges).relabel(**self._relabel).opts(**opts))

        return DynamicMap(distfn, streams=[self.stream])




class HoloViewsSeriesPlot(object):
    """Series plotting accessor and methods
    Examples
    --------
    >>> s.plot.line()
    >>> s.plot.bar()
    >>> s.plot.hist()
    Plotting methods can also be accessed by calling the accessor as a method
    with the ``kind`` argument:
    ``s.plot(kind='line')`` is equivalent to ``s.plot.line()``
    """

    def __init__(self, data):
        self._data = data

    def __call__(self, kind='line', width=800, height=300, backlog=1000,
                 title=None, grid=False, legend=True, logx=False, logy=False,
                 loglog=False, xticks=None, yticks=None, xlim=None, ylim=None,
                 rot=None, fontsize=None, colormap=None, hover=False, **kwds):
        converter = HoloViewsSeriesConverter(
            self._data, kind=kind, width=width, height=height,
            backlog=backlog, title=title, grid=grid, legend=legend,
            logx=logx, logy=logy, hover=hover, loglog=loglog,
            xticks=xticks, yticks=yticks, xlim=xlim, ylim=ylim,
            rot=rot, fontsize=fontsize, colormap=colormap, **kwds
        )
        return converter(kind)

    def line(self, **kwds):
        """
        Line plot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='line', **kwds)

    def scatter(self, **kwds):
        """
        Scatter plot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='scatter', **kwds)

    def bar(self, **kwds):
        """
        Vertical bar plot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='bar', **kwds)

    def barh(self, **kwds):
        """
        Horizontal bar plot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='barh', **kwds)

    def box(self, **kwds):
        """
        Boxplot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='box', **kwds)

    def hist(self, bins=10, **kwds):
        """
        Histogram

        Parameters
        ----------
        bins: integer, default 10
            Number of histogram bins to be used
        bin_range: tuple
            Specifies the range within which to compute the bins,
            defaults to data range
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='hist', bins=bins, **kwds)

    def kde(self, **kwds):
        """
        Kernel Density Estimate plot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='kde', **kwds)

    density = kde

    def area(self, **kwds):
        """
        Area plot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='area', **kwds)

    def table(self, **kwds):
        """
        Table

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.Series.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='table', **kwds)



class HoloViewsFramePlot(object):

    def __init__(self, data):
        self._data = data

    def __call__(self, x=None, y=None, kind='line', backlog=1000,
             width=800, height=300, title=None, grid=False,
             legend=True, logx=False, logy=False, loglog=False,
             xticks=None, yticks=None, xlim=None, ylim=None, rot=None,
             fontsize=None, colormap=None, hover=False, **kwds):
        converter = HoloViewsFrameConverter(
            self._data, width=width, height=height, backlog=backlog,
            title=title, grid=grid, legend=legend, logx=logx,
            logy=logy, loglog=loglog, xticks=xticks, yticks=yticks,
            xlim=xlim, ylim=ylim, rot=rot, fontsize=fontsize,
            colormap=colormap, hover=hover, **kwds
        )
        return converter(kind, x, y)

    def line(self, x=None, y=None, **kwds):
        """
        Line plot

        Parameters
        ----------
        x, y : label or position, optional
            Coordinates for each point.
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(x, y, kind='line', **kwds)

    def scatter(self, x=None, y=None, **kwds):
        """
        Scatter plot

        Parameters
        ----------
        x, y : label or position, optional
            Coordinates for each point.
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(x, y, kind='scatter', **kwds)

    def area(self, x=None, y=None, **kwds):
        """
        Area plot

        Parameters
        ----------
        x, y : label or position, optional
            Coordinates for each point.
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(x, y, kind='area', **kwds)

    def bar(self, x=None, y=None, **kwds):
        """
        Bars plot

        Parameters
        ----------
        x, y : label or position, optional
            Coordinates for each point.
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(x, y, kind='bars', **kwds)

    def barh(self, **kwds):
        """
        Horizontal bar plot

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='barh', **kwds)

    def box(self, by=None, **kwds):
        """
        Boxplot

        Parameters
        ----------
        by : string or sequence
            Column in the DataFrame to group by.
        kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='box', by=by, **kwds)

    def hist(self, by=None, **kwds):
        """
        Histogram

        Parameters
        ----------
        by : string or sequence
            Column in the DataFrame to group by.
        kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='hist', by=by, **kwds)

    def kde(self, by=None, **kwds):
        """
        KDE

        Parameters
        ----------
        by : string or sequence
            Column in the DataFrame to group by.
        kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='kde', by=by, **kwds)

    def table(self, **kwds):
        """
        Table

        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to
            :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='table', **kwds)


# Register plotting interfaces
def df_plot(self):
    return HoloViewsFramePlot(self)

def series_plot(self):
    return HoloViewsSeriesPlot(self)

DataFrame.plot = property(df_plot)
DataFrames.plot = property(df_plot)
Series.plot = property(series_plot)
Seriess.plot = property(series_plot)

hv.extension('bokeh')
