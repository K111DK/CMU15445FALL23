#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> current = root_;
  std::shared_ptr<const TrieNode> next_search_node = root_;

  if(current == nullptr){
    return nullptr;
  }

  if (key.length() == 0) {

    std::shared_ptr<const TrieNodeWithValue<T>> p = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(current);

    if(p== nullptr){
      return nullptr;
    }

    return p->value_.get();

  }

  size_t index = 0;

  while(index < key.length()){

    if(current == nullptr){
      return nullptr;
    }

    bool found = false;
    if(!current->children_.empty()){
      for(const auto &item: current->children_){
        if( item.first == key.at(index)){
          next_search_node = item.second;
          found = true;
          break ;
        }
      }
    }

    if(!found){
      return nullptr;
    }

    current = next_search_node;
    index++;

  }

  auto ret = std::dynamic_pointer_cast< const TrieNodeWithValue<T> >(current);
  if(ret == nullptr){// Bad cast, is not a Node with value
    return nullptr;
  }
  return ret->value_.get();

}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {

  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if(key.length() == 0) {
    std::shared_ptr<const TrieNode> new_root;
    if(root_ != nullptr){
      auto clone = root_->Clone();
      new_root = std::make_shared<const TrieNodeWithValue<T>>(clone->children_, std::make_shared<T>(std::move(value)));
    }else{
      new_root = std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    }
      return Trie(new_root);
  }

  size_t index = 0;
  std::vector<std::shared_ptr<const TrieNode>> modified_node;
  std::vector<char> modified_node_path;


  auto current = root_;
  auto next_search_node= root_;
  if(root_ != nullptr){

    modified_node.push_back(root_);

    while(index < key.length()){
      bool found = false;
      if(!current->children_.empty()){
        for(const auto &item: current->children_){
          if( item.first == key.at(index)){

            modified_node.push_back(item.second);
            modified_node_path.push_back(key.at(index));
            next_search_node = item.second;
            found = true;
            break;

          }
        }
      }

      if(found){

        current = next_search_node;
        index++;

      }else{
        break;
      }
    }

  }else{
    modified_node.push_back(std::make_shared<const TrieNode>());
  }

  // need to insert a child node to current of value = key.at(index)

  std::shared_ptr<const TrieNode> extra_node;
  std::shared_ptr<const TrieNode> lower_node = nullptr;
  //  index/<--/- n -/-->/length
  if(index < key.length()){
    //use the insert val to create new node;
    lower_node = std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    size_t count = key.length() - 1 - index;
    size_t i = 0;
    while(i < count){
        auto new_map = std::map<char, std::shared_ptr<const TrieNode>>{{key.at(key.length() - 1 - i), lower_node}};
        lower_node = std::make_shared<const TrieNode>(new_map);
        i++;
    }
    modified_node_path.push_back(key.at(index));

  }else{
    lower_node = modified_node[modified_node.size() - 1];
    modified_node.pop_back();
    auto clone = lower_node->Clone();
    lower_node = std::make_shared<const TrieNodeWithValue<T>>
        (clone->children_,std::make_shared<T>(std::move(value)));
  }

  while(!modified_node.empty()) {

    auto upper_node = modified_node[modified_node.size() - 1];
    auto upper_2_lower_key = modified_node_path[modified_node.size() - 1];
    auto clone = upper_node->Clone();

    if (lower_node != nullptr){
      clone->children_[upper_2_lower_key] = lower_node;
    }

    if(upper_node->is_value_node_){
      std::shared_ptr<const TrieNodeWithValue<T>> cast = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(upper_node);
      if(cast == nullptr){
        throw Exception("Bad Cast");
      }
    }

    lower_node = std::shared_ptr<const TrieNode>(std::move(clone));

    modified_node_path.pop_back();
    modified_node.pop_back();
  }
  return Trie(lower_node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  auto current = root_;
  auto next_search_node = root_;

  if (key.length() == 0 ) {
    std::shared_ptr<const TrieNode> p = std::make_shared<const TrieNode>(current->Clone()->children_);
    return Trie(p);
  }


  size_t index = 0;
  std::vector<std::shared_ptr<const TrieNode>> modified_node;
  std::vector<char> modified_node_path;

  if(root_ != nullptr){

    modified_node.push_back(root_);

    while(index < key.length()){
      bool found = false;
      if(!current->children_.empty()){
        for(const auto &item: current->children_){
          if( item.first == key.at(index)){

            modified_node.push_back(item.second);
            modified_node_path.push_back(key.at(index));

            next_search_node = item.second;
            found = true;
            break ;
          }
        }
      }

      if(found){

        current = next_search_node;
        index++;

      }else{

        break;

      }
    }

  }else{
    modified_node.push_back(std::make_shared<const TrieNode>());
  }

  // need to insert a child node to current of value = key.at(index)

  std::shared_ptr<const TrieNode> extra_node;
  std::shared_ptr<const TrieNode> lower_node = nullptr;

  if(index < key.length()){
    return *this;
  }

  lower_node = modified_node[modified_node.size() - 1];
  modified_node.pop_back();
  if(lower_node->children_.empty()){
    lower_node = nullptr;
  }else{
    lower_node = std::make_shared<const TrieNode>(lower_node->Clone()->children_);
  }


  while(!modified_node.empty()) {

    auto upper_node = modified_node[modified_node.size() - 1];
    auto upper_2_lower_key = modified_node_path[modified_node.size() - 1];

    auto clone_node = upper_node->Clone();

    if (lower_node != nullptr){
      clone_node->children_[upper_2_lower_key] = lower_node;
    }else{
      clone_node->children_.erase(upper_2_lower_key);
    }

    lower_node = std::shared_ptr<const TrieNode>(std::move(clone_node));

    if( !lower_node->is_value_node_ && lower_node->children_.empty() ){
      lower_node = nullptr;
    }

    modified_node_path.pop_back();
    modified_node.pop_back();
  }

  return Trie(lower_node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
