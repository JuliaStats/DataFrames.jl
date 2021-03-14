module TestDeprecated

using Test, DataFrames

@testset "by and aggregate" begin
    @test_throws ArgumentError by()
    @test_throws ArgumentError aggregate()
end

@testset "All indexing" begin
    df = DataFrame(a=1, b=2, c=3)

    @test select(df, All(1, 2)) == df[:, 1:2]
    @test select(df, All(1, :b)) == df[:, 1:2]
    @test select(df, All(:a, 2)) == df[:, 1:2]
    @test select(df, All(:a, :b)) == df[:, 1:2]
    @test select(df, All(2, 1)) == df[:, [2, 1]]
    @test select(df, All(:b, 1)) == df[:, [2, 1]]
    @test select(df, All(2, :a)) == df[:, [2, 1]]
    @test select(df, All(:b, :a)) == df[:, [2, 1]]

    @test df[:, All(1, 2)] == df[:, 1:2]
    @test df[:, All(1, :b)] == df[:, 1:2]
    @test df[:, All(:a, 2)] == df[:, 1:2]
    @test df[:, All(:a, :b)] == df[:, 1:2]
    @test df[:, All(2, 1)] == df[:, [2, 1]]
    @test df[:, All(:b, 1)] == df[:, [2, 1]]
    @test df[:, All(2, :a)] == df[:, [2, 1]]
    @test df[:, All(:b, :a)] == df[:, [2, 1]]

    @test df[:, All(1, 1, 2)] == df[:, 1:2]
    @test df[:, All(:a, 1, :b)] == df[:, 1:2]
    @test df[:, All(:a, 2, :b)] == df[:, 1:2]
    @test df[:, All(:a, :b, 2)] == df[:, 1:2]
    @test df[:, All(2, 1, :a)] == df[:, [2, 1]]

    @test select(df, All(1, "b")) == df[:, 1:2]
    @test select(df, All("a", 2)) == df[:, 1:2]
    @test select(df, All("a", "b")) == df[:, 1:2]
    @test select(df, All("b", 1)) == df[:, [2, 1]]
    @test select(df, All(2, "a")) == df[:, [2, 1]]
    @test select(df, All("b", "a")) == df[:, [2, 1]]

    @test df[:, All(1, "b")] == df[:, 1:2]
    @test df[:, All("a", 2)] == df[:, 1:2]
    @test df[:, All("a", "b")] == df[:, 1:2]
    @test df[:, All("b", 1)] == df[:, [2, 1]]
    @test df[:, All(2, "a")] == df[:, [2, 1]]
    @test df[:, All("b", "a")] == df[:, [2, 1]]

    @test df[:, All("a", 1, "b")] == df[:, 1:2]
    @test df[:, All("a", 2, "b")] == df[:, 1:2]
    @test df[:, All("a", "b", 2)] == df[:, 1:2]
    @test df[:, All(2, 1, "a")] == df[:, [2, 1]]

    df = DataFrame(a1=1, a2=2, b1=3, b2=4)
    @test df[:, All(r"a", Not(r"1"))] == df[:, [1, 2, 4]]
    @test df[:, All(Not(r"1"), r"a")] == df[:, [2, 4, 1]]
end

@testset "filter and filter! tests with Pair" begin
    @testset "filter() and filter!()" begin
        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter(:x => x -> x > 1, df) == DataFrame(x = [3, 2], y = ["b", "a"])
        @test filter!(:x => x -> x > 1, df) === df == DataFrame(x = [3, 2], y = ["b", "a"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter("x" => x -> x > 1, df) == DataFrame(x = [3, 2], y = ["b", "a"])
        @test filter!("x" => x -> x > 1, df) === df == DataFrame(x = [3, 2], y = ["b", "a"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter(1 => x -> x > 1, df) == DataFrame(x = [3, 2], y = ["b", "a"])
        @test filter!(1 => x -> x > 1, df) === df == DataFrame(x = [3, 2], y = ["b", "a"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter([:x] => x -> x > 1, df) == DataFrame(x = [3, 2], y = ["b", "a"])
        @test filter!([:x] => x -> x > 1, df) === df == DataFrame(x = [3, 2], y = ["b", "a"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter(["x"] => x -> x > 1, df) == DataFrame(x = [3, 2], y = ["b", "a"])
        @test filter!(["x"] => x -> x > 1, df) === df == DataFrame(x = [3, 2], y = ["b", "a"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter((:) => (r...) -> r[1] > 1, df) == DataFrame(x = [3, 2], y = ["b", "a"])
        @test filter!((:) => (r...) -> r[1] > 1, df) === df == DataFrame(x = [3, 2], y = ["b", "a"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter([:x, :x] => ==, df) == df
        @test filter!([:x, :x] => ==, df) === df == DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter(["x", "x"] => ==, df) == df
        @test filter!(["x", "x"] => ==, df) === df == DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
        @test filter([2, 2] => !=, df) == DataFrame(x=Int[], y=String[])
        @test filter!([2, 2] => !=, df) === df == DataFrame(x=Int[], y=String[])

        for sel in [r"x", [1, 2], [:x1, :x2], ["x1", "x2"], :, Not(r"y")]
            df = DataFrame(x1 = [3, 1, 2, 1], x2 = ["b", "c", "aa", "bbb"])
            @test filter(sel => (a, b) -> a == length(b), df) ==
                  DataFrame(x1=[1, 2], x2=["c", "aa"])
            @test filter!(sel => (a, b) -> a == length(b), df) === df ==
                  DataFrame(x1=[1, 2], x2=["c", "aa"])
        end

        @test filter(r"y" => () -> true, df) == df

        df = DataFrame(x = [3, 1, 2, 1, missing], y = ["b", "c", "a", "b", "c"])
        @test_throws ArgumentError filter(:x => x -> x > 1, df)
        @test_throws ArgumentError filter("x" => x -> x > 1, df)
        @test_throws ArgumentError filter!(:x => x -> x > 1, df)
        @test_throws ArgumentError filter!("x" => x -> x > 1, df)
        @test_throws ArgumentError filter(1 => x -> x > 1, df)
        @test_throws ArgumentError filter!(1 => x -> x > 1, df)
        @test_throws ArgumentError filter([:x] => x -> x > 1, df)
        @test_throws ArgumentError filter(["x"] => x -> x > 1, df)
        @test_throws ArgumentError filter!([:x] => x -> x > 1, df)
        @test_throws ArgumentError filter!(["x"] => x -> x > 1, df)
        @test_throws ArgumentError filter((:) => (r...) -> r[1] > 1, df)
        @test_throws ArgumentError filter!((:) => (r...) -> r[1] > 1, df)
    end

    @testset "filter view kwarg test" begin
        df = DataFrame(rand(3, 4), :auto)
        for fun in (:x1 => x -> x > 0, "x1" => x -> x > 0,
                    [:x1] => x -> x > 0, ["x1"] => x -> x > 0,
                    r"1" => x -> x > 0, AsTable(:) => x -> x.x1 > 0)
            @test filter(fun, df) isa DataFrame
            @test filter(fun, view(df, 1:2, 1:2)) isa DataFrame
            @test filter(fun, df, view=false) isa DataFrame
            @test filter(fun, view(df, 1:2, 1:2), view=false) isa DataFrame
            @test filter(fun, df, view=true) isa SubDataFrame
            @test filter(fun, df, view=true) == filter(fun, df)
            @test filter(fun, view(df, 1:2, 1:2), view=true) isa SubDataFrame
            @test filter(fun, view(df, 1:2, 1:2), view=true) == filter(fun, view(df, 1:2, 1:2))
        end
    end

    @testset "filter and filter! with SubDataFrame" begin
        dfv = view(DataFrame(x = [0, 0, 3, 1, 3, 1], y = 1:6), 3:6, 1:1)

        @test filter(:x => x -> x > 2, dfv) == DataFrame(x = [3, 3])
        @test filter(:x => x -> x > 2, dfv, view=true) == DataFrame(x = [3, 3])
        @test parent(filter(:x => x -> x > 2, dfv, view=true)) === parent(dfv)

        @test_throws ArgumentError filter!(:x => x -> x > 2, dfv)
    end

    @testset "filter and filter! with AsTable" begin
        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])

        function testfun(x)
            @assert x isa NamedTuple
            @assert propertynames(x) == (:x,)
            return x.x > 1
        end

        @test filter(AsTable(:x) => testfun, df) == DataFrame(x=[3, 2], y=["b", "a"])
        filter!(AsTable(:x) => testfun, df)
        @test df == DataFrame(x=[3, 2], y=["b", "a"])

        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])

        @test filter(AsTable("x") => testfun, df) == DataFrame(x=[3, 2], y=["b", "a"])
        filter!(AsTable("x") => testfun, df)
        @test df == DataFrame(x=[3, 2], y=["b", "a"])
    end

    @testset "empty arg to filter and filter!" begin
        df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])

        @test filter([] => () -> true, df) == df
        @test filter(AsTable(r"z") => x -> true, df) == df
        @test filter!([] => () -> true, copy(df)) == df
        @test filter!(AsTable(r"z") => x -> true, copy(df)) == df

        flipflop0 = let
            state = false
            () -> (state = !state)
        end

        flipflop1 = let
            state = false
            x -> (state = !state)
        end

        @test filter([] => flipflop0, df) == df[[1, 3], :]
        @test filter(Int[] => flipflop0, df) == df[[1, 3], :]
        @test filter(String[] => flipflop0, df) == df[[1, 3], :]
        @test filter(Symbol[] => flipflop0, df) == df[[1, 3], :]
        @test filter(r"z" => flipflop0, df) == df[[1, 3], :]
        @test filter(Not(All()) => flipflop0, df) == df[[1, 3], :]
        @test filter(Cols() => flipflop0, df) == df[[1, 3], :]
        @test filter(AsTable(r"z") => flipflop1, df) == df[[1, 3], :]
        @test filter(AsTable([]) => flipflop1, df) == df[[1, 3], :]
        @test filter!([] => flipflop0, copy(df)) == df[[1, 3], :]
        @test filter!(Int[] => flipflop0, copy(df)) == df[[1, 3], :]
        @test filter!(String[] => flipflop0, copy(df)) == df[[1, 3], :]
        @test filter!(Symbol[] => flipflop0, copy(df)) == df[[1, 3], :]
        @test filter!(r"z" => flipflop0, copy(df)) == df[[1, 3], :]
        @test filter!(Not(All()) => flipflop0, copy(df)) == df[[1, 3], :]
        @test filter!(Cols() => flipflop0, copy(df)) == df[[1, 3], :]
        @test filter!(AsTable(r"z") => flipflop1, copy(df)) == df[[1, 3], :]
        @test filter!(AsTable([]) => flipflop1, copy(df)) == df[[1, 3], :]

        @test_throws MethodError filter([] => flipflop1, df)
        @test_throws MethodError filter(AsTable([]) => flipflop0, df)
    end

    @testset "GroupedDataFrame filter" begin
        for df in (DataFrame(g1=[1, 3, 2, 1, 4, 1, 2, 5], x1=1:8,
                             g2=[1, 3, 2, 1, 4, 1, 2, 5], x2=1:8),
                   view(DataFrame(g1=[1, 3, 2, 1, 4, 1, 2, 5, 4, 5], x1=1:10,
                                  g2=[1, 3, 2, 1, 4, 1, 2, 5, 4, 5], x2=1:10, y=1:10),
                        1:8, Not(:y)))
            for gcols in (:g1, [:g1, :g2]), cutoff in (1, 0, 10),
                predicate in (1 => x -> length(x) > cutoff,
                              :x1 => x -> length(x) > cutoff,
                              "x1" => x -> length(x) > cutoff,
                              [1, 2] => (x1, x2) -> length(x1) > cutoff,
                              [:x1, :x2] => (x1, x2) -> length(x1) > cutoff,
                              ["x1", "x2"] => (x1, x2) -> length(x1) > cutoff,
                              r"x" => (x1, x2) -> length(x1) > cutoff,
                              AsTable(:x1) => x -> length(x.x1) > cutoff,
                              AsTable(r"x") => x -> length(x.x1) > cutoff)
                gdf1  = groupby(df, gcols)
                gdf2 = filter(predicate, gdf1)
                if cutoff == 1
                    @test getindex.(keys(gdf2), 1) == 1:2
                elseif cutoff == 0
                    @test gdf1 == gdf2
                elseif cutoff == 10
                    @test isempty(gdf2)
                end
            end

            @test_throws ArgumentError filter(r"x" => (x...) -> 1, groupby(df, :g1))
            @test_throws ArgumentError filter(AsTable(r"x") => (x...) -> 1, groupby(df, :g1))

            @test filter(r"y" => (x...) -> true, groupby(df, :g1)) == groupby(df, :g1)
            @test filter([] => (x...) -> true, groupby(df, :g1)) == groupby(df, :g1)
            @test filter(AsTable(r"y") => (x...) -> true, groupby(df, :g1)) == groupby(df, :g1)
            @test filter(AsTable([]) => (x...) -> true, groupby(df, :g1)) == groupby(df, :g1)
        end
    end
end

end # module
