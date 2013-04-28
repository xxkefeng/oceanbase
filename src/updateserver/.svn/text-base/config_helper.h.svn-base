struct CfgItem
{
  CfgItem* last_;
  const char* name_;
  const char* key_;
  int64_t default_value_;
  int64_t value_;
  CfgItem(CfgItem*& last, const char* name, const char* key, int64_t default_value):
    last_(last), name_(name), key_(key), default_value_(default_value), value_(default_value)
  {
    last = this;
  }
  int64_t get() const { return value_; }
  struct CallBack
  {
    CallBack() {}
    virtual ~CallBack() {}
    virtual void call(CfgItem* item) = 0;
  };
  void for_each(CallBack* callable)
  {
    for(CfgItem* p = this; p != NULL; p = p->last_)
    {
      if (callable)callable->call(p);
    }
  }
};

#define DefCfgItem(last, name, value)                                  \
  struct Cfg_ ## name: public CfgItem { Cfg_ ## name(): CfgItem(last, #name, #name, value){}; } name; \
  int64_t get_ ## name() const { return name.get(); }

#if 0
// using example
class YourConfigClass
{
  public:
    DefYourCfg(name, value);
};
// load cfg
refresh_callback.init(...);
CfgT::for_each(refresh_callback);
#endif
