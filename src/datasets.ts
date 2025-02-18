export type DatasetTransform = (row: Record<string, string>) => unknown[];

export type Dataset = {
  name: string;
  file: string;
  columns: string[];
  indexes?: string[];
  transform?: DatasetTransform;
};

export const DATASETS: Dataset[] = [
  {
    name: 'title_basics',
    file: 'title.basics.tsv.gz',
    columns: [
      'tconst TEXT PRIMARY KEY',
      'titleType TEXT',
      'primaryTitle TEXT',
      'originalTitle TEXT',
      'isAdult BOOLEAN',
      'startYear INT',
      'endYear INT',
      'runtimeMinutes INT',
      'genres TEXT',
    ],
    indexes: ['tconst', 'titleType', 'primaryTitle', 'startYear'],
  },
  {
    name: 'title_akas',
    file: 'title.akas.tsv.gz',
    columns: [
      'titleId TEXT',
      'ordering INT',
      'title TEXT',
      'region TEXT',
      'language TEXT',
      'types TEXT',
      'attributes TEXT',
      'isOriginalTitle BOOLEAN',
    ],
    indexes: ['titleId', 'title'],
  },
  {
    name: 'title_ratings',
    file: 'title.ratings.tsv.gz',
    columns: ['tconst TEXT PRIMARY KEY', 'averageRating FLOAT', 'numVotes TEXT'],
    indexes: ['tconst', 'averageRating'],
  },
  {
    name: 'title_crew',
    file: 'title.crew.tsv.gz',
    columns: ['tconst TEXT PRIMARY KEY', 'directors TEXT', 'writers TEXT'],
  },
  {
    name: 'title_episode',
    file: 'title.episode.tsv.gz',
    columns: ['tconst TEXT PRIMARY KEY', 'parentTconst TEXT', 'seasonNumber INT', 'episodeNumber INT'],
    indexes: ['tconst', 'parentTconst', 'seasonNumber', 'episodeNumber'],
  },
  {
    name: 'title_principals',
    file: 'title.principals.tsv.gz',
    columns: ['tconst TEXT', 'ordering INT', 'nconst TEXT', 'category TEXT', 'job TEXT', 'characters TEXT'],
    indexes: ['tconst', 'nconst', 'category'],
  },
  {
    name: 'name_basics',
    file: 'name.basics.tsv.gz',
    columns: [
      'nconst TEXT PRIMARY KEY',
      'primaryName TEXT',
      'birthYear INT',
      'deathYear INT',
      'primaryProfession TEXT',
      'knownForTitles TEXT',
    ],
    indexes: ['nconst', 'primaryName', 'birthYear'],
  },
];
