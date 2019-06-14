CREATE TABLE IF NOT EXISTS wine (
  class Int32,
  alcohol Float64,
  malic_acid Float64,
  ash Float64,
  alcalinity_of_ash Float64,
  magnesium Float64,
  total_phenols Float64,
  flavanoids Float64,
  nonflavanoid_phenols Float64,
  proanthocyanins Float64,
  color_intensity Float64,
  hue Float64,
  od280_od315_of_diluted_wines Float64,
  proline Float64
)
ENGINE = MergeTree
PARTITION BY class
ORDER BY (class)
