import holoviews as hv
import pandas as pd

from holoviews.core.spaces import DynamicMap
from holoviews.core.overlay import NdOverlay
from holoviews.element import (
    Curve, Scatter, Area, Bars, BoxWhisker, Dataset, Distribution
)
from holoviews.streams import Buffer, Pipe

from streamz.dataframe import DataFrame, DataFrames, Series, Seriess


class HoloViewsConverter(object):

    def __init__(self, data, kind=None, by=None, width=800, height=300, backlog=1000, use_index=True,
                 shared_axes=True, grid=False, legend=True, rot=None, title=None, xlim=None,
                 ylim=None, xticks=None, yticks=None, fontsize=None, colormap=None,
                 stacked=False, logx=False, logy=False, loglog=False, **kwds):

        if isinstance(data, (Series, Seriess)):
            data = data.to_frame()
        self.data = data
        self.stream_type = data._stream_type
        if data._stream_type == 'updating':
            self.stream = Pipe(data.example)
        else:
            self.stream = Buffer(data.example, length=backlog)
        data.stream.sink(self.stream.send)
        self.by = by

        if 'cmap' in kwds and colormap:
            raise TypeError("Only specify one of `cmap` and `colormap`.")
        elif 'cmap' in kwds:
            cmap = kwds.pop('cmap')
        else:
            cmap = colormap

        self._plot_opts = {'xticks': xticks, 'yticks': yticks,
                           'yrotation': rot, 'xrotation': rot,
                           'show_grid': grid, 'logx': logx or loglog,
                           'logy': logy or loglog, 'shared_axes': shared_axes,
                           'show_legend': legend, 'width': width, 'height': height}
        self._element_params = {'label': title}
        self._dim_ranges = {'x': xlim, 'y': ylim}
        self._style_opts = {'fontsize': fontsize, 'cmap': cmap}
        self._norm_opts = {'framewise': True}

        self.stacked = stacked
        self.use_index = use_index
        self.kwds = kwds

    def reset_index(self, data):
        if self.stream_type == 'updating':
            return data.reset_index()
        else:
            return data


class HoloViewsFrameConverter(HoloViewsConverter):

    def __call__(self, kind, x, y):
        return getattr(self, kind)(x, y)

    def single_chart(self, chart, x, y):
        opts = dict(plot=self._plot_opts, norm=self._norm_opts)

        def single_chart(data):
            return chart(self.reset_index(data), x, y).opts(**opts)

        return DynamicMap(single_chart, streams=[self.stream])

    def chart(self, chart, x, y):
        if x and y:
            return self.single_chart(chart, x, y)

        def multi_chart(data):
            x = self.data.example.index.name or 'index'
            curves = {}
            opts = dict(plot=self._plot_opts, norm=self._norm_opts)
            for c in data.columns[1:]:
                curves[c] = chart(data, x, c).opts(**opts)
            return NdOverlay(curves)

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

        def bars(data):
            index = self.data.example.index.name or 'index'
            data = self.reset_index(data)
            df = pd.melt(data, id_vars=[index], var_name='Group', value_name='Value')
            opts = {'plot': dict(self._plot_opts, labelled=[]), 'norm': self._norm_opts}
            opts['plot']['stack_index'] = 1 if self.stacked else None
            return Bars(df, kdims=[index, 'Group'], vdims=['Value']).opts(**opts)

        return DynamicMap(bars, streams=[self.stream])

    def barh(self, x, y):
        return self.bars(x, y).opts(plot={'Bars': dict(invert_axes=True)})

    def box(self, x, y):
        if x and y:
            return self.single_chart(BoxWhisker, x, y)

        def box(data):
            index = self.data.example.index.name or 'index'
            data = self.reset_index(data)
            if self.by:
                id_vars = [index, self.by]
                kdims = ['Group', self.by]
            else:
                kdims = ['Group']
                id_vars = [index]
            df = pd.melt(data, id_vars=id_vars, var_name='Group', value_name='Value')
            opts = {'plot': dict(self._plot_opts, labelled=[]),
                    'norm': self._norm_opts}
            opts['plot']['invert_axes'] = not self.kwds.get('vert', True)
            return BoxWhisker(df, kdims, 'Value').opts(**opts)

        return DynamicMap(box, streams=[self.stream])

    def hist(self, x, y):

        def hist(data):
            hists = {}
            data = self.reset_index(data)
            ds = Dataset(data)
            plot_opts = dict(self._plot_opts)
            plot_opts['invert_axes'] = self.kwds.get('orientation', False) == 'horizontal'
            opts = dict(plot=plot_opts, style=dict(alpha=self.kwds.get('alpha', 1)),
                        norm=self._norm_opts)
            hist_opts = {'num_bins': self.kwds.get('bins', 10),
                         'bin_range': self.kwds.get('bin_range', None)}
            for col in data.columns[1:]:
                hists[col] = hv.operation.histogram(ds, dimension=col, **hist_opts).opts(**opts)
            return NdOverlay(hists)

        return DynamicMap(hist, streams=[self.stream])

    def kde(self, x, y):

        def kde(data):
            data = self.reset_index(data)
            index = self.data.example.index.name or 'index'
            df = pd.melt(data, id_vars=[index], var_name='Group', value_name='Value')
            ds = Dataset(df)
            plot_opts = dict(self._plot_opts)
            plot_opts['invert_axes'] = self.kwds.get('orientation', False) == 'horizontal'
            opts = dict(plot=plot_opts, style=dict(alpha=self.kwds.get('alpha', 0.5)),
                        norm=self._norm_opts)
            if len(df):
                overlay = ds.to(Distribution, 'Value').overlay()
            else:
                overlay = NdOverlay({0: Area([], 'Value', 'Value Density')}, ['Group'])
            return overlay.opts({'Distribution': opts, 'Area': opts,
                                 'NdOverlay': {'plot': dict(legend_limit=0)}})

        return DynamicMap(kde, streams=[self.stream])


class HoloViewsSeriesConverter(HoloViewsConverter):

    def __call__(self, kind):
        return getattr(self, kind)()

    def chart(self, chart):
        opts = dict(plot=self._plot_opts, norm=self._norm_opts)

        def chartfn(data):
            if len(data.columns) == 1:
                data = data.reset_index()
            return chart(data).opts(**opts)

        return DynamicMap(chartfn, streams=[self.stream])

    def line(self):
        return self.chart(Curve)

    def scatter(self):
        return self.chart(Scatter)

    def area(self):
        return self.chart(Area)

    def bar(self):
        return self.chart(Bars)

    def box(self):
        opts = dict(plot=self._plot_opts, norm=self._norm_opts)

        def boxfn(data):
            return BoxWhisker(data, [], data.columns[-1]).opts(**opts)

        return DynamicMap(boxfn, streams=[self.stream])

    def hist(self):
        hist_opts = {'num_bins': self.kwds.get('bins', 10),
                     'bin_range': self.kwds.get('bin_range', None)}

        def hist(data):
            ds = Dataset(data)
            plot_opts = dict(self._plot_opts)
            plot_opts['invert_axes'] = self.kwds.get('orientation', False) == 'horizontal'
            opts = dict(plot=plot_opts, style=dict(alpha=self.kwds.get('alpha', 1)),
                        norm=self._norm_opts)
            return hv.operation.histogram(ds, dimension=data.columns[-1],
                                          **hist_opts).opts(**opts)

        return DynamicMap(hist, streams=[self.stream])

    def kde(self):
        plot_opts = dict(self._plot_opts)
        plot_opts['invert_axes'] = self.kwds.get('orientation', False) == 'horizontal'
        opts = dict(plot=plot_opts, style=dict(alpha=self.kwds.get('alpha', 1)),
                    norm=self._norm_opts)

        def distfn(data):
            return Distribution(data, data.columns[-1]).opts(**opts)

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

    def __call__(self, kind='line', width=800, height=300, backlog=1000, use_index=True,
                 title=None, grid=False, legend=True, logx=False, logy=False,
                 loglog=False, xticks=None, yticks=None, xlim=None, ylim=None,
                 rot=None, fontsize=None, colormap=None, table=False, yerr=None,
                 xerr=None, **kwds):
        converter = HoloViewsSeriesConverter(
            self._data, width=width, height=height, backlog=backlog, use_index=use_index, title=title,
            grid=grid, legend=legend, logx=logx, logy=logy, loglog=loglog,
            xticks=xticks, yticks=yticks, xlim=xlim, ylim=ylim, rot=rot,
            fontsize=fontsize, colormap=colormap, yerr=yerr, xerr=xerr, **kwds)
        return converter(kind)

    def line(self, **kwds):
        """
        Line plot
        .. versionadded:: 0.17.0
        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='line', **kwds)

    def scatter(self, **kwds):
        """
        Scatter plot
        .. versionadded:: 0.17.0
        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='scatter', **kwds)

    def bar(self, **kwds):
        """
        Vertical bar plot
        .. versionadded:: 0.17.0
        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='bar', **kwds)

    def barh(self, **kwds):
        """
        Horizontal bar plot
        .. versionadded:: 0.17.0
        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='barh', **kwds)

    def box(self, **kwds):
        """
        Boxplot
        .. versionadded:: 0.17.0
        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='box', **kwds)

    def hist(self, bins=10, **kwds):
        """
        Histogram
        .. versionadded:: 0.17.0
        Parameters
        ----------
        bins: integer, default 10
            Number of histogram bins to be used
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='hist', bins=bins, **kwds)

    def kde(self, **kwds):
        """
        Kernel Density Estimate plot
        .. versionadded:: 0.17.0
        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='kde', **kwds)

    density = kde

    def area(self, **kwds):
        """
        Area plot
        .. versionadded:: 0.17.0
        Parameters
        ----------
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`pandas.Series.plot`.
        Returns
        -------
        axes : matplotlib.AxesSubplot or np.array of them
        """
        return self(kind='area', **kwds)


class HoloViewsFramePlot(object):

    def __init__(self, data):
        self._data = data

    def __call__(self, x=None, y=None, kind='line', backlog=1000, width=800, height=300,
             figsize=None, use_index=True, title=None, grid=False,
             legend=True, logx=False, logy=False, loglog=False,
             xticks=None, yticks=None, xlim=None, ylim=None,
             rot=None, fontsize=None, colormap=None, table=False,
             yerr=None, xerr=None, **kwds):
        converter = HoloViewsFrameConverter(
            self._data, width=width, height=height, backlog=backlog, use_index=use_index, title=title,
            grid=grid, legend=legend, logx=logx, logy=logy, loglog=loglog,
            xticks=xticks, yticks=yticks, xlim=xlim, ylim=ylim, rot=rot,
            fontsize=fontsize, colormap=colormap, yerr=yerr, xerr=xerr, **kwds)
        return converter(kind, x, y)

    def line(self, x=None, y=None, **kwds):
        """
        Line plot

        Parameters
        ----------
        x, y : label or position, optional
            Coordinates for each point.
        **kwds : optional
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
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
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
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
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
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
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
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
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
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
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
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
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
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
            Keyword arguments to pass on to :py:meth:`streamz.dataframe.DataFrame.plot`.
        Returns
        -------
        Element : Element or NdOverlay of Elements
        """
        return self(kind='kde', by=by, **kwds)


def df_plot(self):
    return HoloViewsFramePlot(self)


def series_plot(self):
    return HoloViewsSeriesPlot(self)


DataFrame.plot = property(df_plot)
DataFrames.plot = property(df_plot)
Series.plot = property(series_plot)
Seriess.plot = property(series_plot)

hv.extension('bokeh')
