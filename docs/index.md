---
sd_hide_title: true
---

# Home

```{toctree}
:maxdepth: 2
:caption: Contents:
:hidden:

user_guide/index
api/index
developer_guide/index
```

::::{grid}
:reverse:
:gutter: 3 4 4 4
:margin: 1 2 1 2

:::{grid-item}
:columns: 12 4 4 4

```{image} https://raw.githubusercontent.com/medmodels/graphrecords-static/main/logos/logo_with_background.svg
:class: only-dark
```

```{image} https://raw.githubusercontent.com/medmodels/graphrecords-static/main/logos/logo_without_background.svg
:width: 200px
:class: only-light
```

:::

:::{grid-item}
:columns: 12 8 8 8
:child-align: justify
:class: sd-fs-5

```{rubric} GraphRecords Documentation

```

The GraphRecords documentation is your go-to resource for exploring the package. It offers complete API descriptions and a detailed user guide, giving you everything you need to effectively utilize its features.

```{only} html
![python versions](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)
[![license](https://img.shields.io/github/license/medmodels/graphrecords.svg)](https://github.com/medmodels/graphrecords/blob/main/LICENSE)
[![test passing](https://github.com/medmodels/graphrecords/actions/workflows/testing.yml/badge.svg?branch=main)](https://github.com/medmodels/graphrecords/actions/workflows/testing.yml)
![coverage](https://raw.githubusercontent.com/medmodels/graphrecords-static/refs/heads/main/icons/coverage-badge.svg)
[![PyPI](https://img.shields.io/pypi/v/graphrecords)](https://pypi.org/project/graphrecords/)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
```

:::

::::

---

::::{grid} 1 2 2 3
:gutter: 1 1 1 2

:::{grid-item-card} {material-outlined}`hub;1.5em;sd-mr-1` User Guide
:link-type: doc

The User Guide is your go-to resource for mastering GraphRecords and quickly learning the essentials for working with graph-based data structures.

+++
[Learn more »](./user_guide/index.md)
:::

:::{grid-item-card} {material-outlined}`troubleshoot;1.5em;sd-mr-1` API Reference
:link-type: doc

The API reference provides detailed information on all GraphRecords functionalities and technical interfaces, serving as a quick lookup resource.

+++
[Learn more »](./api/index.md)
:::

:::{grid-item-card} {material-outlined}`school;1.5em;sd-mr-1` Developer Guide
:link-type: doc

The Developer Guide provides comprehensive guidelines on how to contribute to GraphRecords.

+++
[Learn more »](developer_guide/index.md)
:::

::::
