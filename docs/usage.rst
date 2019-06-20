#####
Usage
#####

To use RHG Compute Tools in a project::

    import rhg_compute_tools


Design tools
============

``rhg_compute_tools`` includes various helper functions and templates so you
can more easily create plots in Rhodium and Climate Impact Lab styles.

To use these tools, import the design submodule:

.. code-block:: python

    import rhg_compute_tools.design

When you do this, we'll automatically load the fonts, color palettes, and
default plot specs into matplotlib. There are a couple ways you can use them:

Color schemes
-------------

Use one of the color schemes listed below as a ``cmap`` argument to a plot.
For example, to use the ``rhg_standard`` color scheme in a bar chart:

.. code-block:: python

    >>> data = pd.DataFrame({
    ...     'A': [1, 2, 3, 2, 1],
    ...     'B': [2, 2, 1, 2, 2],
    ...     'C': [5, 4, 3, 2, 1]})

    >>> data.plot(kind='bar', cmap='rhg_standard');

Rhodium Group Color Schemes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Categorical color schemes

* rhg_standard
* rhg_light

Continuous color schemes

* rhg_Blues
* rhg_Greens
* rhg_Yellows
* rhg_Oranges
* rhg_Reds
* rhg_Purples

You can also access the RHG color grid using the array
``rhg_compute_tools.design.colors.RHG_COLOR_GRID``
