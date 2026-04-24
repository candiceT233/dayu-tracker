# About DaYu-Tracker

DaYu-Tracker is a comprehensive HDF5 I/O monitoring and analysis toolkit that provides detailed insights into HDF5 program I/O patterns at multiple levels. It consists of two main components: a Virtual Object Layer (VOL) tracker for object-level operations and a Virtual File Driver (VFD) tracker for low-level POSIX I/O operations.

## Key Features

- **Dual-layer monitoring**: Tracks both HDF5 object operations and underlying file I/O
- **Interactive visualizations**: Generates Sankey diagrams showing data flow between tasks and files
- **Performance analysis**: Identifies bottlenecks and provides optimization recommendations
- **Workflow optimization**: Analyzes data dependencies across entire scientific workflows
- **Low overhead**: Typically under 0.2% runtime and 0.25% storage overhead

## Research Paper

This work was published at **CLUSTER 2024** and demonstrates up to a 3.7x performance improvement in I/O time for obscure bottlenecks.

**Citation:**
```
@inproceedings{tang2024dayu,
  title={DaYu: Optimizing distributed scientific workflows by decoding dataflow semantics and dynamics},
  author={Tang, Meng and Cernuda, Jaime and Ye, Jie and Guo, Luanzheng and Tallent, Nathan R and Kougkas, Anthony and Sun, Xian-He},
  booktitle={2024 IEEE International Conference on Cluster Computing (CLUSTER)},
  pages={357--369},
  year={2024},
  organization={IEEE}
}
```

**Paper PDF:** [http://cs.iit.edu/~scs/assets/files/tang2024dayu.pdf](http://cs.iit.edu/~scs/assets/files/tang2024dayu.pdf)

## Project Website

For more information about this research project, visit: [https://grc.iit.edu/research/projects/dayu](https://grc.iit.edu/research/projects/dayu)

## Funding

This research is supported by the U.S. Department of Energy (DOE) through the Office of Advanced Scientific Computing Research's "Orchestration for Distributed & Data-Intensive Scientific Exploration"; the "Cloud, HPC, and Edge for Science and Security" LDRD at Pacific Northwest National Laboratory; and partly by the National Science Foundation under Grants no. CSSI-2104013 and OAC-2313154.

## License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.
