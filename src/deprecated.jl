import Base: @deprecate

# TODO: remove these definitions when 2.0 release of DataFrames.jl is made

by(args...; kwargs...) = throw(ArgumentError("by function was removed from DataFrames.jl. " *
                                             "Use the `combine(groupby(...), ...)` or `combine(f, groupby(...))` instead."))

aggregate(args...; kwargs...) = throw(ArgumentError("aggregate function was removed from DataFrames.jl. " *
                                                    "Use the `combine` function instead."))

import Base: filter
@deprecate filter((cols, f)::Pair, df::AbstractDataFrame; view::Bool=false) subset(df, cols => ByRow(f), view=view)
@deprecate filter((cols, f)::Pair, gdf::GroupedDataFrame) subset(gdf, cols => f, ungroup=false)

import Base: filter!
@deprecate filter!((cols, f)::Pair, df::AbstractDataFrame) subset!(df, cols => ByRow(f))
