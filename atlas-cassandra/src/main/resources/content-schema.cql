DROP TABLE IF EXISTS content_v2;

DROP TYPE IF EXISTS ReleaseDate;
DROP TYPE IF EXISTS ContainerSummary;
DROP TYPE IF EXISTS Restriction;
DROP TYPE IF EXISTS SegmentEvent;
DROP TYPE IF EXISTS Description;
DROP TYPE IF EXISTS Broadcast;
DROP TYPE IF EXISTS Location;
DROP TYPE IF EXISTS Policy;
DROP TYPE IF EXISTS Pricing;
DROP TYPE IF EXISTS Price;
DROP TYPE IF EXISTS BroadcastRef;
DROP TYPE IF EXISTS LocationSummary;
DROP TYPE IF EXISTS CrewMember;
DROP TYPE IF EXISTS ItemSummary;
DROP TYPE IF EXISTS Tag;
DROP TYPE IF EXISTS KeyPhrase;
DROP TYPE IF EXISTS RelatedLink;
DROP TYPE IF EXISTS Priority;
DROP TYPE IF EXISTS Image;
DROP TYPE IF EXISTS ItemRef;
DROP TYPE IF EXISTS SeriesRef;
DROP TYPE IF EXISTS Synopses;
DROP TYPE IF EXISTS ContainerRef;
DROP TYPE IF EXISTS ContentGroupRef;
DROP TYPE IF EXISTS Ref;
DROP TYPE IF EXISTS Certificate;
DROP TYPE IF EXISTS Alias;
DROP TYPE IF EXISTS Award;

CREATE TYPE Award(
  o text,
  t text,
  d text,
  y int
);

CREATE TYPE Alias (
  n text,
  v text
);

CREATE TYPE Certificate (
  cc text,
  cls text
);

CREATE TYPE Ref (
  id bigint,
  src text
);

CREATE TYPE ContentGroupRef (
  id bigint,
  uri text
);

CREATE TYPE ContainerRef (
  id bigint,
  src text,
  t text,
  tt text,
  upd timestamp,
  sn int,
  ry int,
  cr set<frozen<Certificate>>
);

CREATE TYPE Synopses (
  s text,
  m text,
  l text
);

CREATE TYPE SeriesRef (
  r frozen<Ref>,
  t text,
  u timestamp,
  sn int,
  ry int,
  cr set<frozen<Certificate>>
);

CREATE TYPE ItemRef (
  r frozen<Ref>,
  sk text,
  u timestamp
);

CREATE TYPE Image (
  u text,
  t text,
  c text,
  th text,
  h int,
  w int,
  ar text,
  mt text,
  as timestamp,
  ae timestamp,
  hta boolean,
  s text
);

CREATE TYPE Priority (
  p list<text>,
  n list<text>,
  pr double
);

CREATE TYPE RelatedLink (
  u text,
  t text,
  si text,
  sn text,
  tt text,
  d text,
  i text,
  th text
);

CREATE TYPE KeyPhrase (
  p text,
  w double
);

CREATE TYPE Tag (
  t bigint,
  p text,
  s boolean,
  w float,
  r text,
  o int
);

CREATE TYPE ItemSummary (
  r frozen<ItemRef>,
  t text,
  d text,
  i text,
  ry int,
  c set<frozen<Certificate>>
);

CREATE TYPE CrewMember (
  i bigint,
  c text,
  cu text,
  au set<text>,
  a set<frozen<Alias>>,
  e set<frozen<Ref>>,
  lu timestamp,
  eu timestamp,
  r text,
  n text,
  p text
);

CREATE TYPE LocationSummary (
  a boolean,
  u text,
  s timestamp,
  e timestamp
);

CREATE TYPE BroadcastRef (
  s text,
  c bigint,
  st timestamp,
  e timestamp
);

CREATE TYPE Price (
  c text,
  p int
);

CREATE TYPE Pricing (
  s timestamp,
  e timestamp,
  p frozen<Price>
);

CREATE TYPE Policy (
  i bigint,
  c text,
  cu text,
  au set<text>,
  a set<frozen<Alias>>,
  e set<frozen<Ref>>,
  lu timestamp,
  eu timestamp,
  as timestamp,
  ae timestamp,
  dpf timestamp,
  ac set<text>,
  al int,
  rc text,
  sp set<text>,
  p frozen<Price>,
  pr list<frozen<Pricing>>,
  si bigint,
  pi bigint,
  pl text,
  n text,
  aas timestamp
);

CREATE TYPE Location (
  i bigint,
  c text,
  cu text,
  au set<text>,
  a set<frozen<Alias>>,
  e set<frozen<Ref>>,
  lu timestamp,
  eu timestamp,
  av boolean,
  til boolean,
  tst text,
  tt text,
  u text,
  ec text,
  ei text,
  p frozen<Policy>
);

CREATE TYPE Broadcast (
  i bigint,
  c text,
  cu text,
  au set<text>,
  a set<frozen<Alias>>,
  e set<frozen<Ref>>,  
  lu timestamp,
  eu timestamp,
  ci bigint,
  ts timestamp,
  te timestamp,
  bd bigint,
  sd date,
  sp boolean,
  si text,
  vi text,
  r boolean,
  sub boolean,
  sgn boolean,
  ad boolean,
  hd boolean,
  w boolean,
  sr boolean,
  l boolean,
  ns boolean,
  ne boolean,
  p boolean,
  thd boolean,
  br boolean
);

CREATE TYPE Description (
  t text,
  s text,
  i text,
  th text
);

CREATE TYPE SegmentEvent (
  i bigint,
  c text,
  cu text,
  au set<text>,
  a set<frozen<Alias>>,
  e set<frozen<Ref>>,
  lu timestamp,
  eu timestamp,
  p int,
  o bigint,
  ic boolean,
  d frozen<Description>,
  sr frozen<Ref>,
  vi text,
  pb text
);

CREATE TYPE Restriction (
  i bigint,
  c text,
  cu text,
  au set<text>,
  a set<frozen<Alias>>,
  e set<frozen<Ref>>,
  lu timestamp,
  eu timestamp,
  r boolean,
  ma int,
  msg text,
  ath text,
  rt text
);

CREATE TYPE ContainerSummary (
  t text,
  tt text,
  d text,
  s int
);

CREATE TYPE ReleaseDate (
  r date,
  c text,
  t text
);

CREATE TABLE content_v2 (
  t text,
  id bigint,
  dcr text,
  c text,
  cu text,
  au set<text>,
  a set<frozen<Alias>>,
  e set<frozen<Ref>>,
  lu timestamp,
  eu timestamp,
  tt text,
  sd text,
  md text,
  ld text,
  sn frozen<Synopses>,
  ds text,
  mt text,
  sp text,
  g set<text>,
  pb text,
  im text,
  ims set<frozen<Image>>,
  th text,
  fs timestamp,
  lf timestamp,
  toclu timestamp,
  so boolean,
  ap boolean,
  pc text,
  pr frozen<Priority>,
  rl set<frozen<RelatedLink>>,
  aw set<frozen<Award>>,
  kp set<frozen<KeyPhrase>>,
  tg list<frozen<Tag>>,
  cgr list<frozen<ContentGroupRef>>,
  pp list<frozen<CrewMember>>,
  ln set<text>,
  cr set<frozen<Certificate>>,
  yr int,
  gd boolean,
  er set<frozen<Ref>>,
  
  isrc text,
  dr bigint,

  snb int,
  te int,
  br frozen<Ref>,

  cnr frozen<ContainerRef>,
  ilf boolean,
  bnw boolean,
  coo set<text>,
  sk text,
  cns frozen<ContainerSummary>,
  bc set<frozen<Broadcast>>,
  sev list<frozen<SegmentEvent>>,
  rr set<frozen<Restriction>>,

  wu text,
  sub set<text>,
  rd set<frozen<ReleaseDate>>,

  en int,
  pn int,
  spc boolean,

  ser list<frozen<SeriesRef>>,

  itr list<frozen<ItemRef>>,
  upc map<frozen<ItemRef>, frozen<list<BroadcastRef>>>,
  avc map<frozen<ItemRef>, frozen<list<LocationSummary>>>,
  its list<frozen<ItemSummary>>,

  bl text,

  PRIMARY KEY (id, dcr)
);
