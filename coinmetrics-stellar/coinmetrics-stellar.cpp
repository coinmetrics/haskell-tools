#include <iomanip>
#include <xdrpp/marshal.h>
#include <xdr/Stellar-overlay.h>

// Class for encoding to JSON.
struct JsonArchive
{
  std::ostream& stream;
  bool comma = false;

  JsonArchive(std::ostream& stream) : stream(stream) {}

  void printField(char const* field)
  {
    if(comma) stream << ',';
    else comma = true;
    if(field) escapeString(field) << ':';
  }

  void operator()(char const* field, xdr::xdr_void)
  {
    printField(field);
    stream << "null";
  }

  template <uint32_t N>
  void operator()(char const* field, xdr::xstring<N> const& s)
  {
    printField(field);
    escapeString(s);
  }
  template <uint32_t N>
  void operator()(char const* field, xdr::opaque_array<N> const& v)
  {
    printField(field);
    std::ostream s(stream.rdbuf());
    s << '"';
    for(size_t i = 0; i < v.size(); ++i)
      s << std::hex << std::setw(2) << std::setfill('0') << (int)v[i];
    s << '"';
  }
  template <uint32_t N>
  void operator()(char const* field, xdr::opaque_vec<N> const& v)
  {
    printField(field);
    std::ostream s(stream.rdbuf());
    s << '"';
    for(size_t i = 0; i < v.size(); ++i)
      s << std::hex << std::setw(2) << std::setfill('0') << (int)v[i];
    s << '"';
  }

  template <typename T>
  std::enable_if_t<xdr::xdr_traits<T>::is_enum> operator()(char const* field, T t)
  {
    printField(field);
    char const* name = xdr::xdr_traits<T>::enum_name(t);
    if(name)
      escapeString(name);
    else
      escapeString(std::to_string(t));
  }

  template <typename T>
  std::enable_if_t<xdr::xdr_traits<T>::is_numeric> operator()(char const* field, T t)
  {
    printField(field);
    stream << std::to_string(t);
  }

  template <typename T>
  void operator()(char const* field, std::tuple<T> const& t)
  {
    xdr::archive(*this, std::get<0>(t), field);
  }

  template <typename T>
  void operator()(char const* field, xdr::pointer<T> const& t)
  {
    if(t)
      xdr::archive(*this, *t, field);
    else
    {
      printField(field);
      stream << "null";
    }
  }

  template <typename T>
  std::enable_if_t<xdr::xdr_traits<T>::is_class> operator()(char const* field, T const& t)
  {
    printField(field);
    stream << "{";
    comma = false;
    xdr::xdr_traits<T>::save(*this, t);
    stream << "}";
    comma = true;
  }

  template <typename T>
  std::enable_if_t<xdr::xdr_traits<T>::is_container> operator()(char const* field, T const& t)
  {
    printField(field);
    stream << "[";
    comma = false;
    for(auto const& o : t)
      xdr::archive(*this, o);
    stream << "]";
    comma = true;
  }

  std::ostream& escapeString(std::string const& s)
  {
    stream << '\"';
    for(size_t i = 0; i < s.size(); ++i)
    {
      char c = s[i];
      switch(c)
      {
      case '"':
        stream << "\\\"";
        break;
      case '\\':
        stream << "\\\\";
        break;
      case '\b':
        stream << "\\b";
        break;
      case '\f':
        stream << "\\f";
        break;
      case '\n':
        stream << "\\n";
        break;
      case '\r':
        stream << "\\r";
        break;
      case '\t':
        stream << "\\t";
        break;
      default:
        if(c >= '\x00' && c <= '\x1f')
          stream << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
        else
          stream << c;
        break;
      }
    }
    stream << '\"';
    return stream;
  }
};

namespace xdr
{
  template<>
  struct archive_adapter<JsonArchive>
  {
    template <typename T>
    static void apply(JsonArchive& a, T const& obj, char const* field)
    {
      a(field, obj);
    }
  };
}

// StablePtr J.Value
struct HsValue;
// StablePtr HexString
struct HsHash;
// StablePtr TransactionHistoryEntry
struct HsTransactionHistoryEntry;

// Imported functions.
extern "C" HsValue* coinmetrics_stellar_encode_json(char const* str, size_t len);
extern "C" HsHash* coinmetrics_stellar_hash(char const* data, size_t len);
extern "C" HsTransactionHistoryEntry* coinmetrics_stellar_combine_transaction_history_entry_with_hashes(HsValue* value, HsHash** hashes, size_t hashesCount);

template <typename T>
HsValue* decode(uint8_t const*& begin, uint8_t const* end)
{
  if(begin + 4 >= end) return nullptr;
  T t;
  {
    xdr::xdr_get getter(begin + 4, end);
    getter(t);
    begin = (uint8_t const*)getter.p_;
  }

  std::ostringstream s;
  JsonArchive a(s);
  xdr::archive(a, t);
  std::string json = s.str();
  return coinmetrics_stellar_encode_json(json.data(), json.length());
}

// Exported functions.
extern "C" HsValue* coinmetrics_stellar_decode_ledger_history_entry(uint8_t const** begin, uint8_t const* end)
{
  return decode<stellar::LedgerHeaderHistoryEntry>(*begin, end);
}

extern "C" HsTransactionHistoryEntry* coinmetrics_stellar_decode_transaction_history_entry(uint8_t const** begin, uint8_t const* end, uint8_t const* networkIdData)
{
  if(*begin + 4 >= end) return nullptr;

  stellar::TransactionHistoryEntry t;
  {
    xdr::xdr_get getter(*begin + 4, end);
    getter(t);
    *begin = (uint8_t const*)getter.p_;
  }

  stellar::Hash networkId;
  memcpy(networkId.data(), networkIdData, + 32);

  auto const& txs = t.txSet.txs;
  std::vector<HsHash*> hashes;
  for(size_t i = 0; i < txs.size(); ++i)
  {
    xdr::msg_ptr msg;
    switch(txs[i].type())
    {
    case stellar::ENVELOPE_TYPE_TX_V0:
      // yes, it's fucked up
      msg = xdr::xdr_to_msg(networkId, stellar::ENVELOPE_TYPE_TX, stellar::ENVELOPE_TYPE_TX_V0, txs[i].v0().tx);
      break;
    case stellar::ENVELOPE_TYPE_TX:
      {
        stellar::TransactionSignaturePayload payload;
        payload.networkId = networkId;
        payload.taggedTransaction.type(stellar::ENVELOPE_TYPE_TX).tx() = txs[i].v1().tx;
        msg = xdr::xdr_to_msg(payload);
      }
      break;
    case stellar::ENVELOPE_TYPE_TX_FEE_BUMP:
      {
        stellar::TransactionSignaturePayload payload;
        payload.networkId = networkId;
        payload.taggedTransaction.type(stellar::ENVELOPE_TYPE_TX_FEE_BUMP).feeBump() = txs[i].feeBump().tx;
        msg = xdr::xdr_to_msg(payload);
      }
      break;
    default:
      continue;
    }

    hashes.push_back(coinmetrics_stellar_hash(msg->data(), msg->size()));
  }

  std::ostringstream s;
  JsonArchive a(s);
  xdr::archive(a, t);
  std::string json = s.str();
  return coinmetrics_stellar_combine_transaction_history_entry_with_hashes(coinmetrics_stellar_encode_json(json.data(), json.length()), &*hashes.begin(), hashes.size());
}

extern "C" HsValue* coinmetrics_stellar_decode_result(uint8_t const** begin, uint8_t const* end)
{
  return decode<stellar::TransactionHistoryResultEntry>(*begin, end);
}
