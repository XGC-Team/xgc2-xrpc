#pragma once
#include <algorithm>
#include <ostream>
#include <streambuf>
#include <string>
#include <utility>
namespace xgc2 {
namespace xrpc {
// JSON libraries can serialize into this ostream without first allocating an
// unbounded temporary dump. Overflow sets the ostream failure state.
class BoundedOutput : public std::streambuf {
public:
  explicit BoundedOutput(std::size_t limit) : limit_(limit), stream_(this) {}
  std::ostream &stream() noexcept { return stream_; }
  bool good() const noexcept { return stream_.good(); }
  const std::string &value() const noexcept { return bytes_; }
  std::string take() && noexcept { return std::move(bytes_); }

protected:
  std::streamsize xsputn(const char *data, std::streamsize count) override {
    if (count < 0)
      return 0;
    const auto n = std::min<std::size_t>(limit_ - bytes_.size(),
                                         static_cast<std::size_t>(count));
    bytes_.append(data, n);
    return static_cast<std::streamsize>(n);
  }
  int_type overflow(int_type c) override {
    if (traits_type::eq_int_type(c, traits_type::eof()))
      return traits_type::not_eof(c);
    if (bytes_.size() == limit_)
      return traits_type::eof();
    bytes_ += traits_type::to_char_type(c);
    return c;
  }

private:
  std::size_t limit_;
  std::string bytes_;
  std::ostream stream_;
};
} // namespace xrpc
} // namespace xgc2
